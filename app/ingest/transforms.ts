import {
  PNXEventPipeline,
  AnalyticsEventPipeline,
  HLSEventPipeline,
  PNXEvent,
  AnalyticsEvent,
  HLSEvent,
} from "./models";
import { DeadLetterQueue, MooseCache } from "@514labs/moose-lib";

/** =======PNX Event Processing========= */

// Helper function to parse user agent
function parseUserAgent(userAgent: string): {
  browserName?: string;
  operatingSystem?: string;
} {
  const browserName = userAgent.includes("Chrome")
    ? "Chrome"
    : userAgent.includes("Firefox")
    ? "Firefox"
    : userAgent.includes("Safari")
    ? "Safari"
    : userAgent.includes("Edge")
    ? "Edge"
    : undefined;

  const operatingSystem = userAgent.includes("Windows")
    ? "Windows"
    : userAgent.includes("Macintosh")
    ? "macOS"
    : userAgent.includes("Linux")
    ? "Linux"
    : userAgent.includes("iPhone")
    ? "iOS"
    : userAgent.includes("Android")
    ? "Android"
    : undefined;

  return { browserName, operatingSystem };
}

// Transform PNX events to Analytics events (for analytics type events)
PNXEventPipeline.stream!.addTransform(
  AnalyticsEventPipeline.stream!,
  async (pnxEvent: PNXEvent): Promise<AnalyticsEvent | null> => {
    /**
     * Transform PNX events to Analytics events with caching and enrichment.
     * Only processes events where type === "analytics"
     */

    // Only process analytics events
    if (pnxEvent.type !== "analytics") {
      return null; // Skip non-analytics events
    }

    const cache = await MooseCache.get();
    const cacheKey = `analytics:${pnxEvent.eventId}`;

    // Check cache first
    const cached = await cache.get<AnalyticsEvent>(cacheKey);
    if (cached) {
      console.log(`Using cached analytics result for ${pnxEvent.eventId}`);
      return cached;
    }

    const { browserName, operatingSystem } = parseUserAgent(pnxEvent.userAgent);

    const result: AnalyticsEvent = {
      eventId: pnxEvent.eventId,
      appName: pnxEvent.appName,
      appVersion: pnxEvent.appVersion,
      userId: pnxEvent.userId,
      userName: pnxEvent.userName,
      eventType: pnxEvent.event,
      eventSource: pnxEvent.source,
      navigationHref: pnxEvent.href,
      timestamp: new Date(pnxEvent.requestTimeEpoch),
      domainName: pnxEvent.domainName,
      stage: pnxEvent.stage,
      sourceIp: pnxEvent.sourceIp,
      userAgent: pnxEvent.userAgent,
      browserName,
      operatingSystem,
    };

    // Cache for 1 hour
    await cache.set(cacheKey, result, 3600);

    return result;
  }
  // {
  //   deadLetterQueue: PNXEventPipeline.deadLetterQueue,
  // }
);

// Transform PNX events to HLS events (for hls type events)
PNXEventPipeline.stream!.addTransform(
  HLSEventPipeline.stream!,
  async (pnxEvent: PNXEvent): Promise<HLSEvent | null> => {
    /**
     * Transform PNX events to HLS events with caching and enrichment.
     * Only processes events where type === "hls"
     */

    // Only process HLS events
    if (pnxEvent.type !== "hls") {
      return null; // Skip non-HLS events
    }

    const cache = await MooseCache.get();
    const cacheKey = `hls:${pnxEvent.eventId}`;

    // Check cache first
    const cached = await cache.get<HLSEvent>(cacheKey);
    if (cached) {
      console.log(`Using cached HLS result for ${pnxEvent.eventId}`);
      return cached;
    }

    const { browserName, operatingSystem } = parseUserAgent(pnxEvent.userAgent);

    const result: HLSEvent = {
      eventId: pnxEvent.eventId,
      appName: pnxEvent.appName,
      appVersion: pnxEvent.appVersion,
      userId: pnxEvent.userId,
      userName: pnxEvent.userName,
      eventType: pnxEvent.event,
      level: pnxEvent.level,
      videoIndex: pnxEvent.videoIndex,
      bitrate: pnxEvent.bitrate,
      resolution: pnxEvent.resolution,
      bandwidth: pnxEvent.bandwidth,
      fragmentDuration: pnxEvent.fragmentDuration,
      timestamp: new Date(pnxEvent.requestTimeEpoch),
      domainName: pnxEvent.domainName,
      stage: pnxEvent.stage,
      sourceIp: pnxEvent.sourceIp,
      userAgent: pnxEvent.userAgent,
      browserName,
      operatingSystem,
    };

    // Cache for 1 hour
    await cache.set(cacheKey, result, 3600);

    return result;
  }
  // {
  //   deadLetterQueue: PNXEventPipeline.deadLetterQueue,
  // }
);

// Add consumers to log incoming PNX events
const printPNXEvent = (pnxEvent: PNXEvent): void => {
  console.log("Received PNX event:");
  console.log(`  Event ID: ${pnxEvent.eventId}`);
  console.log(`  App: ${pnxEvent.appName} v${pnxEvent.appVersion}`);
  console.log(`  User: ${pnxEvent.userName} (${pnxEvent.userId})`);
  console.log(`  Type: ${pnxEvent.type}`);
  console.log(`  Event: ${pnxEvent.event}`);
  console.log(`  Stage: ${pnxEvent.stage}`);
  console.log(`  Timestamp: ${new Date(pnxEvent.requestTimeEpoch)}`);
  console.log("---");
};

PNXEventPipeline.stream!.addConsumer(printPNXEvent);

// DLQ consumer for PNX events
// PNXEventPipeline.deadLetterQueue!.addConsumer((deadLetter) => {
//   console.log("PNX Event failed processing:");
//   console.log(deadLetter);
//   const pnxEvent: PNXEvent = deadLetter.asTyped();
//   console.log("Failed PNX Event:", pnxEvent);
// });
