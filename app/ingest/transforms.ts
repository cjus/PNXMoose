import {
  PNXEventPipeline,
  AnalyticsEventPipeline,
  HLSEventPipeline,
  // NavigationEventPipeline, // removed; navigation is analytics
  AuthenticationEventPipeline,
  MetricEventPipeline,
  ErrorEventPipeline,
  PNXEvent,
  AnalyticsEvent,
  HLSEvent,
  // NavigationEvent, // removed
  AuthenticationEvent,
  MetricEvent,
  ErrorEvent,
} from "./models";
import { MooseCache } from "@514labs/moose-lib";
import { createHash } from "node:crypto";

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
    if (cached) return null;

    const { browserName, operatingSystem } = parseUserAgent(pnxEvent.userAgent);

    // Auto-derive eventSource from appName and event context when source is not provided
    const eventSource =
      pnxEvent.eventSource || pnxEvent.source || deriveEventSource(pnxEvent);

    const result: AnalyticsEvent = {
      eventId: pnxEvent.eventId,
      appName: pnxEvent.appName,
      appVersion: pnxEvent.appVersion,
      userId: pnxEvent.userId,
      sessionId: pnxEvent.sessionId,
      visitorId: pnxEvent.visitorId,
      pageviewId: pnxEvent.pageviewId,
      eventType: pnxEvent.event,
      description: pnxEvent.description,
      eventSource,
      navigationHref: pnxEvent.navigationHref || pnxEvent.href,
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
    if (cached) return null;

    const { browserName, operatingSystem } = parseUserAgent(pnxEvent.userAgent);

    const result: HLSEvent = {
      eventId: pnxEvent.eventId,
      appName: pnxEvent.appName,
      appVersion: pnxEvent.appVersion,
      userId: pnxEvent.userId,
      sessionId: pnxEvent.sessionId,
      visitorId: pnxEvent.visitorId,
      pageviewId: pnxEvent.pageviewId,
      eventType: pnxEvent.event,
      description: pnxEvent.description,
      level: pnxEvent.level,
      videoIndex: pnxEvent.videoIndex,
      videoId: pnxEvent.videoId,
      bitrate: pnxEvent.bitrate,
      resolution: pnxEvent.resolution,
      bandwidth: pnxEvent.bandwidth,
      fragmentDuration: pnxEvent.fragmentDuration,
      availableLevelsJson: (pnxEvent as any).availableLevels
        ? JSON.stringify((pnxEvent as any).availableLevels)
        : pnxEvent.availableLevelsJson,
      totalLevels: pnxEvent.totalLevels,
      detailJson: (pnxEvent as any).detail
        ? JSON.stringify((pnxEvent as any).detail)
        : pnxEvent.detailJson,
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

// (Navigation transform removed) Navigation is now represented as AnalyticsEvent

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
    if (cached) return null;

    const { browserName, operatingSystem } = parseUserAgent(pnxEvent.userAgent);

    const result: AuthenticationEvent = {
      eventId: pnxEvent.eventId,
      appName: pnxEvent.appName,
      appVersion: pnxEvent.appVersion,
      userId: pnxEvent.userId,
      sessionId: pnxEvent.sessionId,
      visitorId: pnxEvent.visitorId,
      pageviewId: pnxEvent.pageviewId,
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
    if (cached) return null;

    const { browserName, operatingSystem } = parseUserAgent(pnxEvent.userAgent);

    // Parse metric data from JSON
    let parsedMetricData: Record<string, unknown> = {};
    if ((pnxEvent as any).metricData) {
      parsedMetricData = (pnxEvent as any).metricData as Record<
        string,
        unknown
      >;
    } else if (pnxEvent.metricDataJson) {
      try {
        parsedMetricData = JSON.parse(pnxEvent.metricDataJson) as Record<
          string,
          unknown
        >;
      } catch (error) {
        console.warn(
          `Failed to parse metricDataJson for event ${pnxEvent.eventId}:`,
          error
        );
      }
    }

    // Extract specific metric fields with proper typing
    const reason =
      typeof parsedMetricData.reason === "string"
        ? parsedMetricData.reason
        : undefined;
    const startWallClockMs =
      typeof parsedMetricData.startWallClockMs === "number"
        ? parsedMetricData.startWallClockMs
        : undefined;
    const endWallClockMs =
      typeof parsedMetricData.endWallClockMs === "number"
        ? parsedMetricData.endWallClockMs
        : undefined;
    const durationMs =
      typeof parsedMetricData.durationMs === "number"
        ? parsedMetricData.durationMs
        : undefined;
    const startPositionSec =
      typeof parsedMetricData.startPositionSec === "number"
        ? parsedMetricData.startPositionSec
        : undefined;
    const endPositionSec =
      typeof parsedMetricData.endPositionSec === "number"
        ? parsedMetricData.endPositionSec
        : undefined;
    const totalDurationSec =
      typeof parsedMetricData.totalDurationSec === "number"
        ? parsedMetricData.totalDurationSec
        : undefined;

    const result: MetricEvent = {
      eventId: pnxEvent.eventId,
      appName: pnxEvent.appName,
      appVersion: pnxEvent.appVersion,
      userId: pnxEvent.userId,
      sessionId: pnxEvent.sessionId,
      visitorId: pnxEvent.visitorId,
      pageviewId: pnxEvent.pageviewId,
      videoId: (pnxEvent as any).videoId,
      eventType: pnxEvent.event,
      description: pnxEvent.description,
      // Transformed metric data fields
      reason,
      startWallClockMs,
      endWallClockMs,
      durationMs,
      startPositionSec,
      endPositionSec,
      totalDurationSec,
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
    if (cached) return null;

    const { browserName, operatingSystem } = parseUserAgent(pnxEvent.userAgent);

    // === Derivation helpers ===
    const rawMessage = (pnxEvent as any).message as string | undefined;
    const rawStack = (pnxEvent as any).stack as string | undefined;

    // Simple classifier
    const classify = (message?: string, stack?: string): string => {
      if (message && message.includes("play() request was interrupted"))
        return "media_play_interrupted";
      if (message && message.toLowerCase().includes("network"))
        return "network_error";
      if (message && message.toLowerCase().includes("timeout"))
        return "timeout_error";
      if (stack && stack.startsWith("TypeError"))
        return "javascript_type_error";
      if (stack && stack.startsWith("ReferenceError"))
        return "javascript_reference_error";
      return "javascript_error";
    };

    // Parse top frame from stack
    let fileName: string | undefined;
    let line: number | undefined;
    let column: number | undefined;
    if (rawStack) {
      const lines = rawStack.split(/\n+/);
      const top = lines.find(
        (l) => l.includes(":") && (l.includes("/") || l.includes("\\"))
      );
      if (top) {
        const match =
          top.match(/(\/[^\s\)]+):(\d+):(\d+)/) ||
          top.match(/(\w:\\[^\s\)]+):(\d+):(\d+)/);
        if (match) {
          fileName = match[1];
          line = Number(match[2]);
          column = Number(match[3]);
        }
      }
    }

    const eventType = classify(rawMessage, rawStack);
    const urlPath = (pnxEvent as any).href
      ? String((pnxEvent as any).href).replace(/^https?:\/\/[^/]+/, "")
      : undefined;
    const description =
      [eventType, fileName && `${fileName}:${line}:${column}`]
        .filter(Boolean)
        .join(" – ") ||
      rawMessage ||
      undefined;
    const dedupeKey =
      [eventType, fileName, line, column].filter(Boolean).join("|") ||
      undefined;
    const severity: "info" | "warn" | "error" = eventType.includes(
      "interrupted"
    )
      ? "warn"
      : "error";

    const result: ErrorEvent = {
      eventId: pnxEvent.eventId,
      appName: pnxEvent.appName,
      appVersion: pnxEvent.appVersion,
      userId: pnxEvent.userId,
      sessionId: pnxEvent.sessionId,
      visitorId: pnxEvent.visitorId,
      pageviewId: pnxEvent.pageviewId,
      eventType,
      description,
      errorMessage: pnxEvent.errorMessage ?? rawMessage,
      stackTrace: pnxEvent.stackTrace ?? rawStack,
      errorClass: rawStack ? rawStack.split(":")[0] : undefined,
      fileName,
      line,
      column,
      urlPath,
      dedupeKey,
      severity,
      contextJson: undefined,
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

// Minimal consumer retained for potential future hooks; no noisy logs
PNXEventPipeline.stream!.addConsumer(() => {});

// DLQ consumer for PNX events
// PNXEventPipeline.deadLetterQueue!.addConsumer((deadLetter) => {
//   console.log("PNX Event failed processing:");
//   console.log(deadLetter);
//   const pnxEvent: PNXEvent = deadLetter.asTyped();
//   console.log("Failed PNX Event:", pnxEvent);
// });
