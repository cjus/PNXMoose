import {
  PNXEventPipeline,
  AnalyticsEventPipeline,
  HLSEventPipeline,
  NavigationEventPipeline,
  AuthenticationEventPipeline,
  MetricEventPipeline,
  ErrorEventPipeline,
  PNXEvent,
  AnalyticsEvent,
  HLSEvent,
  NavigationEvent,
  AuthenticationEvent,
  MetricEvent,
  ErrorEvent,
} from "./models";
import { MooseCache } from "@514labs/moose-lib";

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

// Helper function to derive eventSource from event context when source is not provided
function deriveEventSource(pnxEvent: PNXEvent): string {
  // First, try to derive from appName
  const appSourceMapping: Record<string, string> = {
    pnxplayer: "pnxplayer-client",
    "PNXStudios-Website": "website",
    "PNXStudios-Admin": "admin-dashboard",
    "pnxstudios-website": "website",
  };

  const baseSource = appSourceMapping[pnxEvent.appName] || "unknown-app";

  // For website events, try to be more specific based on event patterns
  if (baseSource === "website") {
    const event = pnxEvent.event;
    const href = pnxEvent.href;

    if (
      event === "navigation_link_clicked" ||
      event === "tracked_link_clicked"
    ) {
      return "website-navigation";
    }

    if (href && (href.includes("/dashboard") || href.includes("/admin"))) {
      return "website-dashboard";
    }

    if (
      event.includes("signup") ||
      event.includes("login") ||
      event.includes("auth")
    ) {
      return "website-auth";
    }

    if (event.includes("form") || event.includes("submit")) {
      return "website-forms";
    }

    return "website-general";
  }

  return baseSource;
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

    // Auto-derive eventSource from appName and event context when source is not provided
    const eventSource = pnxEvent.source || deriveEventSource(pnxEvent);

    const result: AnalyticsEvent = {
      eventId: pnxEvent.eventId,
      appName: pnxEvent.appName,
      appVersion: pnxEvent.appVersion,
      userId: pnxEvent.userId,
      sessionId: pnxEvent.sessionId,
      eventType: pnxEvent.event,
      description: pnxEvent.description,
      eventSource,
      navigationHref: pnxEvent.href,
      videoId: pnxEvent.videoId,
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
      sessionId: pnxEvent.sessionId,
      eventType: pnxEvent.event,
      description: pnxEvent.description,
      level: pnxEvent.level,
      videoIndex: pnxEvent.videoIndex,
      videoId: pnxEvent.videoId,
      bitrate: pnxEvent.bitrate,
      resolution: pnxEvent.resolution,
      bandwidth: pnxEvent.bandwidth,
      fragmentDuration: pnxEvent.fragmentDuration,
      availableLevels: pnxEvent.availableLevels
        ? JSON.stringify(pnxEvent.availableLevels)
        : undefined,
      totalLevels: pnxEvent.totalLevels,
      detail: pnxEvent.detail ? JSON.stringify(pnxEvent.detail) : undefined,
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

// Transform PNX events to Navigation events (for navigation type events)
PNXEventPipeline.stream!.addTransform(
  NavigationEventPipeline.stream!,
  async (pnxEvent: PNXEvent): Promise<NavigationEvent | null> => {
    // Only process navigation events
    if (pnxEvent.type !== "navigation") {
      return null;
    }

    const cache = await MooseCache.get();
    const cacheKey = `navigation:${pnxEvent.eventId}`;

    const cached = await cache.get<NavigationEvent>(cacheKey);
    if (cached) {
      console.log(`Using cached navigation result for ${pnxEvent.eventId}`);
      return cached;
    }

    const { browserName, operatingSystem } = parseUserAgent(pnxEvent.userAgent);

    const result: NavigationEvent = {
      eventId: pnxEvent.eventId,
      appName: pnxEvent.appName,
      appVersion: pnxEvent.appVersion,
      userId: pnxEvent.userId,
      sessionId: pnxEvent.sessionId,
      eventType: pnxEvent.event,
      description: pnxEvent.description,
      action: pnxEvent.action,
      href: pnxEvent.href,
      eventName: pnxEvent.eventName,
      eventProps: pnxEvent.eventProps
        ? JSON.stringify(pnxEvent.eventProps)
        : undefined,
      timestamp: new Date(pnxEvent.requestTimeEpoch),
      domainName: pnxEvent.domainName,
      stage: pnxEvent.stage,
      sourceIp: pnxEvent.sourceIp,
      userAgent: pnxEvent.userAgent,
      browserName,
      operatingSystem,
    };

    await cache.set(cacheKey, result, 3600);
    return result;
  }
);

// Transform PNX events to Authentication events (for authentication type events)
PNXEventPipeline.stream!.addTransform(
  AuthenticationEventPipeline.stream!,
  async (pnxEvent: PNXEvent): Promise<AuthenticationEvent | null> => {
    // Only process authentication events
    if (pnxEvent.type !== "authentication") {
      return null;
    }

    const cache = await MooseCache.get();
    const cacheKey = `auth:${pnxEvent.eventId}`;

    const cached = await cache.get<AuthenticationEvent>(cacheKey);
    if (cached) {
      console.log(`Using cached authentication result for ${pnxEvent.eventId}`);
      return cached;
    }

    const { browserName, operatingSystem } = parseUserAgent(pnxEvent.userAgent);

    const result: AuthenticationEvent = {
      eventId: pnxEvent.eventId,
      appName: pnxEvent.appName,
      appVersion: pnxEvent.appVersion,
      userId: pnxEvent.userId,
      sessionId: pnxEvent.sessionId,
      eventType: pnxEvent.event,
      description: pnxEvent.description,
      action: pnxEvent.action,
      method: pnxEvent.method,
      timestamp: new Date(pnxEvent.requestTimeEpoch),
      domainName: pnxEvent.domainName,
      stage: pnxEvent.stage,
      sourceIp: pnxEvent.sourceIp,
      userAgent: pnxEvent.userAgent,
      browserName,
      operatingSystem,
    };

    await cache.set(cacheKey, result, 3600);
    return result;
  }
);

// Transform PNX events to Metric events (for metric type events)
PNXEventPipeline.stream!.addTransform(
  MetricEventPipeline.stream!,
  async (pnxEvent: PNXEvent): Promise<MetricEvent | null> => {
    // Only process metric events
    if (pnxEvent.type !== "metric") {
      return null;
    }

    const cache = await MooseCache.get();
    const cacheKey = `metric:${pnxEvent.eventId}`;

    const cached = await cache.get<MetricEvent>(cacheKey);
    if (cached) {
      console.log(`Using cached metric result for ${pnxEvent.eventId}`);
      return cached;
    }

    const { browserName, operatingSystem } = parseUserAgent(pnxEvent.userAgent);

    // Extract additional metric data (exclude base fields)
    const metricData: any = {};
    const baseFields = [
      "eventId",
      "appName",
      "appVersion",
      "userId",
      "type",
      "event",
      "description",
      "requestTimeEpoch",
      "domainName",
      "stage",
      "sourceIp",
      "userAgent",
    ];

    Object.keys(pnxEvent).forEach((key) => {
      if (!baseFields.includes(key) && (pnxEvent as any)[key] !== undefined) {
        metricData[key] = (pnxEvent as any)[key];
      }
    });

    const result: MetricEvent = {
      eventId: pnxEvent.eventId,
      appName: pnxEvent.appName,
      appVersion: pnxEvent.appVersion,
      userId: pnxEvent.userId,
      sessionId: pnxEvent.sessionId,
      eventType: pnxEvent.event,
      description: pnxEvent.description,
      metricData: JSON.stringify(metricData),
      timestamp: new Date(pnxEvent.requestTimeEpoch),
      domainName: pnxEvent.domainName,
      stage: pnxEvent.stage,
      sourceIp: pnxEvent.sourceIp,
      userAgent: pnxEvent.userAgent,
      browserName,
      operatingSystem,
    };

    await cache.set(cacheKey, result, 3600);
    return result;
  }
);

// Transform PNX events to Error events (for error type events)
PNXEventPipeline.stream!.addTransform(
  ErrorEventPipeline.stream!,
  async (pnxEvent: PNXEvent): Promise<ErrorEvent | null> => {
    // Only process error events
    if (pnxEvent.type !== "error") {
      return null;
    }

    const cache = await MooseCache.get();
    const cacheKey = `error:${pnxEvent.eventId}`;

    const cached = await cache.get<ErrorEvent>(cacheKey);
    if (cached) {
      console.log(`Using cached error result for ${pnxEvent.eventId}`);
      return cached;
    }

    const { browserName, operatingSystem } = parseUserAgent(pnxEvent.userAgent);

    const result: ErrorEvent = {
      eventId: pnxEvent.eventId,
      appName: pnxEvent.appName,
      appVersion: pnxEvent.appVersion,
      userId: pnxEvent.userId,
      sessionId: pnxEvent.sessionId,
      eventType: pnxEvent.event,
      description: pnxEvent.description ?? pnxEvent.message,
      errorMessage: pnxEvent.message,
      stackTrace: pnxEvent.stack,
      timestamp: new Date(pnxEvent.requestTimeEpoch),
      domainName: pnxEvent.domainName,
      stage: pnxEvent.stage,
      sourceIp: pnxEvent.sourceIp,
      userAgent: pnxEvent.userAgent,
      browserName,
      operatingSystem,
    };

    await cache.set(cacheKey, result, 3600);
    return result;
  }
);

// Add consumers to log incoming PNX events
const printPNXEvent = (pnxEvent: PNXEvent): void => {
  console.log("Received PNX event:");
  console.log(`  Event ID: ${pnxEvent.eventId}`);
  console.log(`  App: ${pnxEvent.appName} v${pnxEvent.appVersion}`);
  console.log(`  User: ${pnxEvent.userId}`);
  console.log(`  Type: ${pnxEvent.type}`);
  console.log(`  Event: ${pnxEvent.event}`);
  console.log(`  Stage: ${pnxEvent.stage}`);
  console.log(`  Timestamp: ${new Date(pnxEvent.requestTimeEpoch)}`);
  if (pnxEvent.videoId) {
    console.log(`  Video ID: ${pnxEvent.videoId}`);
  }
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
