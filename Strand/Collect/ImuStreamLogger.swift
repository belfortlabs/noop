import Foundation
import WhoopProtocol
import WhoopStore

/// Continuous 100 Hz IMU logger for WHOOP 4.0 and 5.0/MG: decodes each live 1-second, 6-axis IMU
/// frame and writes the samples into a user-chosen folder as one CSV per logging interval, named by
/// the strap family and the interval's UTC start time (e.g. `whoop5-imu-20261008T075304Z.csv`).
/// Interval 0 is "continuous": one CSV per session, named by its first second, with each second's rows
/// appended as the frame arrives.
///
/// WHOOP 5.0/MG frames are the 1,244-byte raw-IMU buffer decoded by `Whoop5RawImu` (strap seconds @15,
/// accel @28/@228/@428, gyro @640/@840/@1040, same scales); that layout has no mapped subsecond or HR
/// field, so those two columns are left empty.
///
/// WHOOP 4.0 frame layout (realtime type-43, 1,917-byte IMU variant, frame-absolute, declared length 1,924):
///   @4 type 43 · @11 u32 LE device seconds · @15 u16 LE subseconds (unit not calibrated) · @21 HR
///   (offsets per whoop_protocol.json REALTIME_RAW_DATA; docs/PROTOCOL_SENSORS.md lists them 4 bytes late)
///   @89/@289/@489 100×i16 accel X/Y/Z (1/4096 g) · @692/@892/@1092 100×i16 gyro X/Y/Z (2000/32768 °/s)
/// Both scales are commonly applied interpretations, not confirmed device parameters; the raw i16
/// counts are logged alongside so nothing is lost if the scale turns out wrong.
@MainActor
final class ImuStreamLogger: ObservableObject {
    /// The most recent decoded second of IMU data, for the Live tab's accelerometer/gyro cards.
    struct Snapshot {
        let receivedAt: Date
        /// [x, y, z] × 100 samples, in g and °/s.
        let accel: [[Double]]
        let gyro: [[Double]]
    }

    static let shared = ImuStreamLogger()

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
    /// `whoop4` / `whoop5`, from the session's first frame; prefixes every CSV name.
    private var filePrefix = "whoop4"
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

    /// One decoded second: strap seconds, optional subsecond/HR, and [ax, ay, az, gx, gy, gz] × 100 raw counts.
    private struct Second {
        let deviceTs: UInt32
        let subsec: Int?
        let hr: Int?
        let columns: [[Int16]]
    }

    /// Consume one WHOOP 4.0 notify frame if it is a CRC-valid type-43 IMU frame. `clockRef` maps the
    /// strap's device seconds to wall time; without it the first frame's host time anchors the session.
    func ingest(_ f: [UInt8], crcOK: Bool, clockRef: ClockRef?) {
        guard f.count > 4, f[0] == 0xAA, f[4] == 43 else { return }
        guard crcOK, Int(f[1]) | Int(f[2]) << 8 == Self.declaredLength,
              f.count >= Self.gyroOffs[2] + 2 * Self.sampleCount else {
            if isActive { framesSkipped += 1 }
            return
        }
        let columns = (Self.accelOffs + Self.gyroOffs).map { off in
            (0..<Self.sampleCount).map { i16(f, off + 2 * $0) }
        }
        consume(Second(deviceTs: u32(f, Self.tsOff),
                       subsec: Int(f[Self.subsecOff]) | Int(f[Self.subsecOff + 1]) << 8,
                       hr: Int(f[Self.hrOff]), columns: columns),
                family: "whoop4", clockRef: clockRef)
    }

    /// Consume one live WHOOP 5.0/MG notify frame if it is a CRC-valid 1,244-byte raw-IMU buffer.
    /// Callers must not pass history-offload frames: banked buffers would land in today's CSVs.
    func ingestWhoop5(_ f: [UInt8], clockRef: ClockRef?) {
        guard f.count == Whoop5RawImu.bufferLength else { return }
        guard verifyFrame(f, family: .whoop5).ok, let raw = Whoop5RawImu.rawColumns(f),
              let ts = Whoop5RawImu.baseTs(f) else {
            if isActive { framesSkipped += 1 }
            return
        }
        let n = Whoop5RawImu.sampleCount
        let columns = (0..<6).map { c in Array(raw[(c * n)..<((c + 1) * n)]) }
        consume(Second(deviceTs: UInt32(truncatingIfNeeded: ts), subsec: nil, hr: nil, columns: columns),
                family: "whoop5", clockRef: clockRef)
    }

    private func consume(_ sec: Second, family: String, clockRef: ClockRef?) {
        // The strap can resend a second; keep strictly one block per device second.
        if sec.deviceTs == lastDeviceTs { return }
        lastDeviceTs = sec.deviceTs
        latest = Snapshot(
            receivedAt: Date(),
            accel: sec.columns[0..<3].map { $0.map { Double($0) * Self.accelScale } },
            gyro: sec.columns[3..<6].map { $0.map { Double($0) * Self.gyroScale } })
        guard isActive else { return }
        if framesWritten == 0 { filePrefix = family }
        let hostMs = Int64(Date().timeIntervalSince1970 * 1_000)
        let offset = clockRef.map { $0.wall - $0.device } ?? wallOffset ?? (Int(hostMs / 1_000) - Int(sec.deviceTs))
        wallOffset = offset
        let wallSec = Int(sec.deviceTs) + offset
        let rows = rows(sec, wallSec: wallSec, hostMs: hostMs)
        framesWritten += 1
        if intervalSeconds == 0 { appendContinuous(rows, wallSec: wallSec); return }
        let bucket = wallSec - wallSec % intervalSeconds
        if let pendingBucket, pendingBucket != bucket { flushPending() }
        pendingBucket = bucket
        pendingRows += rows
        // Close the file as soon as its last second has arrived rather than waiting for the next one.
        if wallSec == bucket + intervalSeconds - 1 { flushPending() }
    }

    private func rows(_ sec: Second, wallSec: Int, hostMs: Int64) -> String {
        let subsec = sec.subsec.map(String.init) ?? "", hr = sec.hr.map(String.init) ?? ""
        let n = sec.columns[0].count
        var out = ""
        out.reserveCapacity(n * 120)
        for i in 0..<n {
            let a = sec.columns[0..<3].map { $0[i] }, g = sec.columns[3..<6].map { $0[i] }
            let wall = String(format: "%.2f", Double(wallSec) + Double(i) / Double(n))
            let scaled = a.map { String(format: "%.5f", Double($0) * Self.accelScale) }
                + g.map { String(format: "%.3f", Double($0) * Self.gyroScale) }
            out += "\(hostMs),\(sec.deviceTs),\(subsec),\(wall),\(i),\(scaled.joined(separator: ",")),"
                + "\((a + g).map(String.init).joined(separator: ",")),\(hr)\n"
        }
        return out
    }

    private func flushPending() {
        defer { pendingBucket = nil; pendingRows = "" }
        guard let bucket = pendingBucket, !pendingRows.isEmpty, let folder else { return }
        let stamp = Self.nameFormatter.string(from: Date(timeIntervalSince1970: TimeInterval(bucket)))
        let name = "\(filePrefix)-imu-\(stamp).csv"
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
            let name = "\(filePrefix)-imu-\(Self.nameFormatter.string(from: Date(timeIntervalSince1970: TimeInterval(wallSec)))).csv"
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
