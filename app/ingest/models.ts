import { IngestPipeline, Key } from "@514labs/moose-lib";

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
  type: string;
  event: string;
  description?: string; // Human readable description of the event
  // Analytics-specific fields (optional)
  source?: string; // e.g., "panel-manager". If not provided, will be auto-derived from appName
  href?: string; // For navigation events
  videoId?: string; // Video identifier for analytics and HLS events
  // HLS video-specific fields (optional)
  level?: number;
  videoIndex?: number;
  bitrate?: number;
  resolution?: string;
  bandwidth?: number;
  fragmentDuration?: number;
  // Additional HLS diagnostics/metadata (optional)
  availableLevels?: any; // forwarded raw; transformed to JSON string downstream
  totalLevels?: number;
  detail?: any; // forwarded raw; transformed to JSON string downstream
  // Navigation-specific fields (optional)
  action?: string; // e.g., "nav_link_clicked"
  eventName?: string; // For tracked links
  eventProps?: any; // Additional event properties
  // Authentication-specific fields (optional)
  method?: string; // e.g., "email_password", "google_oauth"
  // Error-specific fields (optional)
  message?: string; // Error message
  stack?: string; // Error stack trace
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
  availableLevels?: string; // JSON stringified array of available ABR levels
  totalLevels?: number;
  detail?: string; // JSON stringified error/detail payloads
  timestamp: Date; // From requestTimeEpoch
  domainName: string;
  stage: string;
  sourceIp: string;
  userAgent: string;
  browserName?: string; // Parsed from userAgent
  operatingSystem?: string; // Parsed from userAgent
}

/** Processed navigation event */
export interface NavigationEvent {
  eventId: Key<string>; // From PNXEvent.eventId
  appName: string;
  appVersion: string;
  userId: string;
  sessionId: string;
  eventType: string; // From message.event
  description?: string; // Optional human readable description
  action?: string; // From message.action
  href?: string; // From message.href
  eventName?: string; // From message.eventName
  eventProps?: string; // JSON stringified eventProps
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
  eventType: string; // From message.event
  description?: string; // Optional human readable description
  metricData: string; // JSON stringified additional metric properties
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
  eventType: string; // From message.event
  description?: string; // Optional human readable description
  errorMessage?: string; // From message.message
  stackTrace?: string; // From message.stack
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
  table: false, // No table; only stream raw events
  stream: true, // Buffer ingested events
  ingest: true, // POST /ingest/PNXEvent
  // deadLetterQueue: {
  //   destination: pnxEventDeadLetterTable,
  // },
});

/** Analytics events processing and storage */
export const AnalyticsEventPipeline = new IngestPipeline<AnalyticsEvent>(
  "AnalyticsEvent",
  {
    table: true, // Persist in ClickHouse table "AnalyticsEvent"
    stream: true, // Buffer processed events
    ingest: false, // No direct API; only derive from PNXEvent
  }
);

/** HLS video events processing and storage */
export const HLSEventPipeline = new IngestPipeline<HLSEvent>("HLSEvent", {
  table: true, // Persist in ClickHouse table "HLSEvent"
  stream: true, // Buffer processed events
  ingest: false, // No direct API; only derive from PNXEvent
});

/** Navigation events processing and storage */
export const NavigationEventPipeline = new IngestPipeline<NavigationEvent>(
  "NavigationEvent",
  {
    table: true, // Persist in ClickHouse table "NavigationEvent"
    stream: true, // Buffer processed events
    ingest: false, // No direct API; only derive from PNXEvent
  }
);

/** Authentication events processing and storage */
export const AuthenticationEventPipeline =
  new IngestPipeline<AuthenticationEvent>("AuthenticationEvent", {
    table: true, // Persist in ClickHouse table "AuthenticationEvent"
    stream: true, // Buffer processed events
    ingest: false, // No direct API; only derive from PNXEvent
  });

/** Metric events processing and storage */
export const MetricEventPipeline = new IngestPipeline<MetricEvent>(
  "MetricEvent",
  {
    table: true, // Persist in ClickHouse table "MetricEvent"
    stream: true, // Buffer processed events
    ingest: false, // No direct API; only derive from PNXEvent
  }
);

/** Error events processing and storage */
export const ErrorEventPipeline = new IngestPipeline<ErrorEvent>("ErrorEvent", {
  table: true, // Persist in ClickHouse table "ErrorEvent"
  stream: true, // Buffer processed events
  ingest: false, // No direct API; only derive from PNXEvent
});
