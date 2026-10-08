import Foundation
import WhoopStore

/// Continuous WHOOP 4.0 100 Hz IMU logger: decodes each realtime type-43 frame in the 1,917-byte
/// IMU layout (docs/PROTOCOL_SENSORS.md "WHOOP 4 realtime raw layouts") and writes the samples into
/// a user-chosen folder as one CSV per logging interval, named by the interval's UTC start time
/// (e.g. `whoop4-imu-20261008T075304Z.csv`). Interval 0 is "continuous": one CSV per session, named by
/// its first second, with each second's rows appended as the frame arrives.
///
/// Frame layout (WHOOP 4 frame-absolute offsets, declared length 1,924):
///   @4 type 43 · @11 u32 LE device seconds · @15 u16 LE subseconds (unit not calibrated) · @21 HR
///   (offsets per whoop_protocol.json REALTIME_RAW_DATA; docs/PROTOCOL_SENSORS.md lists them 4 bytes late)
///   @89/@289/@489 100×i16 accel X/Y/Z (1/4096 g) · @692/@892/@1092 100×i16 gyro X/Y/Z (2000/32768 °/s)
/// Both scales are commonly applied interpretations, not confirmed device parameters; the raw i16
/// counts are logged alongside so nothing is lost if the scale turns out wrong.
@MainActor
final class Whoop4ImuStreamLogger: ObservableObject {
    /// The most recent decoded second of IMU data, for the Live tab's accelerometer/gyro cards.
    struct Snapshot {
        let receivedAt: Date
        /// [x, y, z] × 100 samples, in g and °/s.
        let accel: [[Double]]
        let gyro: [[Double]]
    }

    static let shared = Whoop4ImuStreamLogger()

    static let declaredLength = 1_924, sampleCount = 100
    static let accelScale = 1.0 / 4096.0, gyroScale = 2000.0 / 32768.0
    /// Seconds per CSV; 0 = continuous (one file per session).
    static let intervalChoices = [0, 1, 5, 10, 30, 60]
    private static let tsOff = 11, subsecOff = 15, hrOff = 21
    private static let accelOffs = [89, 289, 489], gyroOffs = [692, 892, 1092]
    static let header = "host_ms,device_s,subsec_raw,wall_s,i,ax_g,ay_g,az_g,gx_dps,gy_dps,gz_dps,"
        + "ax_raw,ay_raw,az_raw,gx_raw,gy_raw,gz_raw,hr_bpm\n"
    private static let bookmarkKey = "whoop4-imu-log-folder-bookmark"
    private static let intervalKey = "whoop4-imu-log-interval-s"
    private static let nameFormatter: DateFormatter = {
        let f = DateFormatter()
        f.locale = Locale(identifier: "en_US_POSIX"); f.timeZone = TimeZone(identifier: "UTC")
        f.dateFormat = "yyyyMMdd'T'HHmmss'Z'"
        return f
    }()

    /// The user-chosen destination folder (security-scoped while the app runs).
    @Published private(set) var folder: URL?
    @Published private(set) var isActive = false
    /// Seconds of IMU data received this session, and CSV files written.
    @Published private(set) var framesWritten = 0
    @Published private(set) var filesWritten = 0
    /// Type-43 frames that were not the IMU layout (e.g. the 1,921-byte optical variant) or failed CRC.
    @Published private(set) var framesSkipped = 0
    @Published private(set) var lastFileName: String?
    @Published private(set) var lastError: String?
    /// Updated for every IMU frame that arrives, whether or not a log session is running.
    @Published private(set) var latest: Snapshot?
    @Published var intervalSeconds: Int {
        didSet { UserDefaults.standard.set(intervalSeconds, forKey: Self.intervalKey) }
    }

    private var lastDeviceTs: UInt32?
    /// Device-seconds → wall-seconds offset for this session (clock correlation, else host time).
    private var wallOffset: Int?
    private var pendingBucket: Int?
    private var pendingRows = ""
    /// Continuous mode: the open session file, created on the first frame.
    private var continuousHandle: FileHandle?

    private init() {
        let saved = UserDefaults.standard.integer(forKey: Self.intervalKey)
        intervalSeconds = Self.intervalChoices.contains(saved) ? saved : 1
        restoreFolder()
    }

    /// Adopt a folder the user just picked (from `fileImporter`) and remember it across launches.
    func setFolder(_ url: URL) {
        folder?.stopAccessingSecurityScopedResource()
        _ = url.startAccessingSecurityScopedResource()
        folder = url
        if let data = try? url.bookmarkData(options: Self.bookmarkOptions, includingResourceValuesForKeys: nil,
                                            relativeTo: nil) {
            UserDefaults.standard.set(data, forKey: Self.bookmarkKey)
        }
    }

    private func restoreFolder() {
        guard let data = UserDefaults.standard.data(forKey: Self.bookmarkKey) else { return }
        var stale = false
        guard let url = try? URL(resolvingBookmarkData: data, options: Self.resolveOptions,
                                 relativeTo: nil, bookmarkDataIsStale: &stale),
              url.startAccessingSecurityScopedResource() else {
            UserDefaults.standard.removeObject(forKey: Self.bookmarkKey); return
        }
        folder = url
        if stale { setFolder(url) }
    }

    #if os(macOS)
    private static let bookmarkOptions: URL.BookmarkCreationOptions = [.withSecurityScope]
    private static let resolveOptions: URL.BookmarkResolutionOptions = [.withSecurityScope]
    #else
    private static let bookmarkOptions: URL.BookmarkCreationOptions = []
    private static let resolveOptions: URL.BookmarkResolutionOptions = []
    #endif

    /// Begin a session. Returns false when no folder has been chosen.
    @discardableResult
    func start() -> Bool {
        guard folder != nil else { return false }
        framesWritten = 0; filesWritten = 0; framesSkipped = 0; lastFileName = nil; lastError = nil
        lastDeviceTs = nil; wallOffset = nil; pendingBucket = nil; pendingRows = ""
        isActive = true
        return true
    }

    func stop() {
        flushPending()
        try? continuousHandle?.close()
        continuousHandle = nil
        isActive = false
    }

    /// Consume one notify frame if it is a CRC-valid WHOOP 4 type-43 IMU frame. `clockRef` maps the
    /// strap's device seconds to wall time; without it the first frame's host time anchors the session.
    func ingest(_ f: [UInt8], crcOK: Bool, clockRef: ClockRef?) {
        guard f.count > 4, f[0] == 0xAA, f[4] == 43 else { return }
        guard crcOK, Int(f[1]) | Int(f[2]) << 8 == Self.declaredLength,
              f.count >= Self.gyroOffs[2] + 2 * Self.sampleCount else {
            if isActive { framesSkipped += 1 }
            return
        }
        let deviceTs = u32(f, Self.tsOff)
        // The strap can resend a second; keep strictly one block per device second.
        if deviceTs == lastDeviceTs { return }
        lastDeviceTs = deviceTs
        latest = Snapshot(
            receivedAt: Date(),
            accel: Self.accelOffs.map { off in (0..<Self.sampleCount).map { Double(i16(f, off + 2 * $0)) * Self.accelScale } },
            gyro: Self.gyroOffs.map { off in (0..<Self.sampleCount).map { Double(i16(f, off + 2 * $0)) * Self.gyroScale } })
        guard isActive else { return }
        let hostMs = Int64(Date().timeIntervalSince1970 * 1_000)
        let offset = clockRef.map { $0.wall - $0.device } ?? wallOffset ?? (Int(hostMs / 1_000) - Int(deviceTs))
        wallOffset = offset
        let wallSec = Int(deviceTs) + offset
        let rows = rows(f, deviceTs: deviceTs, wallSec: wallSec, hostMs: hostMs)
        framesWritten += 1
        if intervalSeconds == 0 { appendContinuous(rows, wallSec: wallSec); return }
        let bucket = wallSec - wallSec % intervalSeconds
        if let pendingBucket, pendingBucket != bucket { flushPending() }
        pendingBucket = bucket
        pendingRows += rows
        // Close the file as soon as its last second has arrived rather than waiting for the next one.
        if wallSec == bucket + intervalSeconds - 1 { flushPending() }
    }

    private func rows(_ f: [UInt8], deviceTs: UInt32, wallSec: Int, hostMs: Int64) -> String {
        let subsec = Int(f[Self.subsecOff]) | Int(f[Self.subsecOff + 1]) << 8
        let hr = f[Self.hrOff]
        var out = ""
        out.reserveCapacity(Self.sampleCount * 120)
        for i in 0..<Self.sampleCount {
            let a = Self.accelOffs.map { i16(f, $0 + 2 * i) }
            let g = Self.gyroOffs.map { i16(f, $0 + 2 * i) }
            let wall = String(format: "%.2f", Double(wallSec) + Double(i) / Double(Self.sampleCount))
            let scaled = a.map { String(format: "%.5f", Double($0) * Self.accelScale) }
                + g.map { String(format: "%.3f", Double($0) * Self.gyroScale) }
            out += "\(hostMs),\(deviceTs),\(subsec),\(wall),\(i),\(scaled.joined(separator: ",")),"
                + "\((a + g).map(String.init).joined(separator: ",")),\(hr)\n"
        }
        return out
    }

    private func flushPending() {
        defer { pendingBucket = nil; pendingRows = "" }
        guard let bucket = pendingBucket, !pendingRows.isEmpty, let folder else { return }
        let stamp = Self.nameFormatter.string(from: Date(timeIntervalSince1970: TimeInterval(bucket)))
        let name = "whoop4-imu-\(stamp).csv"
        let url = folder.appendingPathComponent(name)
        do {
            try Data((Self.header + pendingRows).utf8).write(to: url, options: .atomic)
            filesWritten += 1; lastFileName = name
        } catch {
            lastError = "\(name): \(error.localizedDescription)"
        }
    }

    private func appendContinuous(_ rows: String, wallSec: Int) {
        if continuousHandle == nil {
            guard let folder else { return }
            let name = "whoop4-imu-\(Self.nameFormatter.string(from: Date(timeIntervalSince1970: TimeInterval(wallSec)))).csv"
            let url = folder.appendingPathComponent(name)
            guard FileManager.default.createFile(atPath: url.path, contents: Data(Self.header.utf8)),
                  let handle = try? FileHandle(forWritingTo: url) else {
                lastError = "\(name): could not create file"; return
            }
            _ = try? handle.seekToEnd()
            continuousHandle = handle
            filesWritten = 1; lastFileName = name
        }
        do { try continuousHandle?.write(contentsOf: Data(rows.utf8)) } catch {
            lastError = "\(lastFileName ?? "CSV"): \(error.localizedDescription)"
        }
    }

    private func u32(_ f: [UInt8], _ o: Int) -> UInt32 {
        UInt32(f[o]) | UInt32(f[o + 1]) << 8 | UInt32(f[o + 2]) << 16 | UInt32(f[o + 3]) << 24
    }
    private func i16(_ f: [UInt8], _ o: Int) -> Int16 { Int16(bitPattern: UInt16(f[o]) | UInt16(f[o + 1]) << 8) }
}
