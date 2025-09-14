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
  userName: string;
  type: string;
  event: string;
  // Analytics-specific fields (optional)
  source?: string; // e.g., "panel-manager"
  href?: string; // For navigation events
  // HLS video-specific fields (optional)
  level?: number;
  videoIndex?: number;
  bitrate?: number;
  resolution?: string;
  bandwidth?: number;
  fragmentDuration?: number;
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
  userName: string;
  eventType: string; // From message.event
  eventSource?: string; // From message.source
  navigationHref?: string; // From message.href
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
  userName: string;
  eventType: string; // From message.event
  level?: number;
  videoIndex?: number;
  bitrate?: number;
  resolution?: string;
  bandwidth?: number;
  fragmentDuration?: number;
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
