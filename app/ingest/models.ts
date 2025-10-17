import { IngestPipeline, ETLPipeline, Key } from "@514labs/moose-lib";

/**
 * Data Pipeline: PNX Events → Analytics Processing
 * Raw Events → HTTP → Event Stream → Transform → Analytics → Processed Stream → DB Table
 */

/** =======Data Models========= */

/** Raw PNX event ingested via API - flattened structure for Moose compatibility */
export interface PNXEvent {
  eventId: string; // Unique event ID
  // Message fields (flattened from nested structure)
  appName: string;
  appVersion: string;
  userId: string;
  sessionId: string; // normalized from session_id/sessionId; "undefined" if not provided by website
  visitorId?: string; // from visitor_id
  pageviewId?: string; // from pageview_id
  type: string;
  event: string;
  description?: string; // Human readable description of the event
  // Analytics-specific fields (optional)
  // New normalized fields from ingest
  eventSource?: string; // Preferred: normalized analytics source
  navigationHref?: string; // Preferred: normalized navigation href
  // Backward-compatibility with older producers/ingestors
  source?: string; // e.g., "panel-manager". If not provided, will be auto-derived from appName
  href?: string; // Legacy navigation href
  videoId?: string; // Video identifier for analytics and HLS events
  // HLS video-specific fields (optional)
  level?: number;
  videoIndex?: number;
  bitrate?: number;
  resolution?: string;
  bandwidth?: number;
  fragmentDuration?: number;
  // Additional HLS diagnostics/metadata (optional)
  availableLevelsJson?: string; // JSON of array/object
  totalLevels?: number;
  detailJson?: string; // JSON of detail payloads
  // Navigation-specific fields (optional)
  action?: string; // e.g., "nav_link_clicked"
  eventName?: string; // For tracked links
  eventPropsJson?: string; // JSON of additional event properties
  // Authentication-specific fields (optional)
  method?: string; // e.g., "email_password", "google_oauth"
  // Error-specific fields (optional)
  // New normalized error fields
  errorMessage?: string; // Preferred
  stackTrace?: string; // Preferred
  // Backward-compatibility
  message?: string; // Legacy error message
  stack?: string; // Legacy error stack trace
  // Metric-specific (optional)
  metricDataJson?: string; // JSON of arbitrary metric payload
  // Request metadata
  requestTimeEpoch: number;
  domainName: string;
  stage: string;
  sourceIp: string;
  userAgent: string;
}

/** Processed analytics event */
export interface AnalyticsEvent {
  eventId: Key<string>; // From PNXEvent.eventId
  appName: string;
  appVersion: string;
  userId: string;
  sessionId: string;
  visitorId?: string;
  pageviewId?: string;
  eventType: string; // From message.event
  description?: string; // Optional human readable description
  eventSource?: string; // From message.source or auto-derived from appName/event context
  navigationHref?: string; // From message.href
  videoId?: string; // Video identifier for player events
  timestamp: Date; // From requestTimeEpoch
  domainName: string;
  stage: string;
  sourceIp: string;
  userAgent: string;
  browserName?: string; // Parsed from userAgent
  operatingSystem?: string; // Parsed from userAgent
}

/** Processed HLS video event */
export interface HLSEvent {
  eventId: Key<string>; // From PNXEvent.eventId
  appName: string;
  appVersion: string;
  userId: string;
  sessionId: string;
  visitorId?: string;
  pageviewId?: string;
  eventType: string; // From message.event
  description?: string; // Optional human readable description
  level?: number;
  videoIndex?: number;
  videoId?: string; // Video identifier for HLS events
  bitrate?: number;
  resolution?: string;
  bandwidth?: number;
  fragmentDuration?: number;
  // Additional HLS diagnostics/metadata
  availableLevelsJson?: string; // JSON stringified array of available ABR levels
  totalLevels?: number;
  detailJson?: string; // JSON stringified error/detail payloads
  timestamp: Date; // From requestTimeEpoch
  domainName: string;
  stage: string;
  sourceIp: string;
  userAgent: string;
  browserName?: string; // Parsed from userAgent
  operatingSystem?: string; // Parsed from userAgent
}

/** Processed authentication event */
export interface AuthenticationEvent {
  eventId: Key<string>; // From PNXEvent.eventId
  appName: string;
  appVersion: string;
  userId: string;
  sessionId: string;
  visitorId?: string;
  pageviewId?: string;
  eventType: string; // From message.event
  description?: string; // Optional human readable description
  action?: string; // From message.action
  method?: string; // From message.method
  timestamp: Date; // From requestTimeEpoch
  domainName: string;
  stage: string;
  sourceIp: string;
  userAgent: string;
  browserName?: string; // Parsed from userAgent
  operatingSystem?: string; // Parsed from userAgent
}

/** Processed metric event */
export interface MetricEvent {
  eventId: Key<string>; // From PNXEvent.eventId
  appName: string;
  appVersion: string;
  userId: string;
  sessionId: string;
  visitorId?: string;
  pageviewId?: string;
  videoId?: string; // Promote common field for metrics (e.g., playback spans)
  eventType: string; // From message.event
  description?: string; // Optional human readable description
  // Transformed metric data fields
  reason?: string; // e.g., "pause"
  startWallClockMs?: number; // Wall clock timestamp when metric started
  endWallClockMs?: number; // Wall clock timestamp when metric ended
  durationMs?: number; // Duration in milliseconds
  startPositionSec?: number; // Start position in seconds
  endPositionSec?: number; // End position in seconds
  totalDurationSec?: number; // Total duration in seconds
  timestamp: Date; // From requestTimeEpoch
  domainName: string;
  stage: string;
  sourceIp: string;
  userAgent: string;
  browserName?: string; // Parsed from userAgent
  operatingSystem?: string; // Parsed from userAgent
}

/** Processed error event */
export interface ErrorEvent {
  eventId: Key<string>; // From PNXEvent.eventId
  appName: string;
  appVersion: string;
  userId: string;
  sessionId: string;
  visitorId?: string;
  pageviewId?: string;
  eventType: string; // From message.event
  description?: string; // Optional human readable description (derived)
  errorMessage?: string; // From message.message
  stackTrace?: string; // From message.stack
  // Derived enrichment
  errorClass?: string;
  fileName?: string;
  line?: number;
  column?: number;
  urlPath?: string;
  dedupeKey?: string;
  severity?: string;
  contextJson?: string; // any extra context captured at producer or inferred here
  timestamp: Date; // From requestTimeEpoch
  domainName: string;
  stage: string;
  sourceIp: string;
  userAgent: string;
  browserName?: string; // Parsed from userAgent
  operatingSystem?: string; // Parsed from userAgent
}

/** =======Pipeline Configuration========= */

/** Dead letter tables for error handling */
// Temporarily commented out to isolate build issue
// export const pnxEventDeadLetterTable = new OlapTable<DeadLetterModel>(
//   "PNXEventDeadLetter",
//   {
//     orderByFields: ["failedAt"],
//   }
// );

/** Raw PNX event ingestion */
export const PNXEventPipeline = new IngestPipeline<PNXEvent>("PNXEvent", {
  table: {
    orderByFields: ["requestTimeEpoch", "domainName", "stage"],
  },
  // Enable HTTP ingestion with a minimal handler; validate/transform as needed
  ingestApi: { path: "ingest/PNXEvent" },
  stream: true, // Buffer ingested events
  // deadLetterQueue: {
  //   destination: pnxEventDeadLetterTable,
  // },
});

/** Analytics events processing and storage */
export const AnalyticsEventPipeline = new IngestPipeline<AnalyticsEvent>(
  "AnalyticsEvent",
  {
    table: { orderByFields: ["eventId", "timestamp"] },
    stream: true,
    ingestApi: false,
  }
);

/** HLS video events processing and storage */
export const HLSEventPipeline = new IngestPipeline<HLSEvent>("HLSEvent", {
  table: { orderByFields: ["eventId", "timestamp"] },
  stream: true,
  ingestApi: false,
});

// (NavigationEvent removed) Navigation is now represented as AnalyticsEvent with source="website-navigation"

/** Authentication events processing and storage */
export const AuthenticationEventPipeline =
  new IngestPipeline<AuthenticationEvent>("AuthenticationEvent", {
    table: { orderByFields: ["eventId", "timestamp"] },
    stream: true,
    ingestApi: false,
  });

/** Metric events processing and storage */
export const MetricEventPipeline = new IngestPipeline<MetricEvent>(
  "MetricEvent",
  {
    table: { orderByFields: ["eventId", "timestamp"] },
    stream: true,
    ingestApi: false,
  }
);

/** Error events processing and storage */
export const ErrorEventPipeline = new IngestPipeline<ErrorEvent>("ErrorEvent", {
  table: { orderByFields: ["eventId", "timestamp"] },
  stream: true,
  ingestApi: false,
});
