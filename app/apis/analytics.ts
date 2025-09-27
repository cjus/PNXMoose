import { Api, MooseCache } from "@514labs/moose-lib";
import { tags } from "typia";

// Query parameters for analytics API
interface AnalyticsQueryParams {
  appName?: string;
  eventType?: string;
  stage?: "production" | "staging" | "development";
  startDate?: string; // YYYY-MM-DD format
  endDate?: string; // YYYY-MM-DD format
  limit?: number & tags.Type<"int32">;
}

// Response data structure
interface AnalyticsResponseData {
  eventDate: string;
  appName: string;
  eventType: string;
  stage: string;
  uniqueSessions: number;
  uniqueVisitors: number;
  pageviews: number;
  totalEvents: number;
  uniqueUsers: number;
  uniqueIPs: number;
  chromeUsers: number;
  safariUsers: number;
  firefoxUsers: number;
  macOSUsers: number;
  windowsUsers: number;
  navigationEvents: number;
}

export const AnalyticsApi = new Api<
  AnalyticsQueryParams,
  AnalyticsResponseData[]
>(
  "analytics",
  async (
    { appName, eventType, stage, startDate, endDate, limit = 100 },
    { client, sql }
  ) => {
    const cache = await MooseCache.get();
    const cacheKey = `analytics:${appName || "all"}:${eventType || "all"}:${
      stage || "all"
    }:${startDate || ""}:${endDate || ""}:${limit}`;

    // Try cache first
    const cachedData = await cache.get<AnalyticsResponseData[]>(cacheKey);
    if (cachedData && Array.isArray(cachedData) && cachedData.length > 0) {
      return cachedData;
    }

    // Build dynamic WHERE clause
    const conditions: string[] = [];

    if (appName) {
      conditions.push(`appName = '${appName}'`);
    }

    if (eventType) {
      conditions.push(`eventType = '${eventType}'`);
    }

    if (stage) {
      conditions.push(`stage = '${stage}'`);
    }

    if (startDate) {
      conditions.push(`eventDate >= '${startDate}'`);
    }

    if (endDate) {
      conditions.push(`eventDate <= '${endDate}'`);
    }

    // Use raw AnalyticsEvent table since materialized view isn't populated yet
    const query = sql`
      SELECT 
        toString(toDate(timestamp)) as eventDate,
        appName,
        eventType,
        stage,
        toInt32(uniq(sessionId)) as uniqueSessions,
        toInt32(uniq(visitorId)) as uniqueVisitors,
        toInt32(uniq(pageviewId)) as pageviews,
        toInt32(count(*)) as totalEvents,
        toInt32(uniq(userId)) as uniqueUsers,
        toInt32(uniq(sourceIp)) as uniqueIPs,
        toInt32(countIf(browserName = 'Chrome')) as chromeUsers,
        toInt32(countIf(browserName = 'Safari')) as safariUsers,
        toInt32(countIf(browserName = 'Firefox')) as firefoxUsers,
        toInt32(countIf(operatingSystem = 'macOS')) as macOSUsers,
        toInt32(countIf(operatingSystem = 'Windows')) as windowsUsers,
        toInt32(countIf(navigationHref IS NOT NULL)) as navigationEvents
      FROM AnalyticsEvent
      GROUP BY toDate(timestamp), appName, eventType, stage
      ORDER BY eventDate DESC, totalEvents DESC
      LIMIT 30
    `;

    try {
      const data = await client.query.execute<AnalyticsResponseData>(query);
      const result: AnalyticsResponseData[] = await data.json();

      // Convert string values to numbers for proper dashboard display
      const convertedResult = result.map((item) => ({
        ...item,
        uniqueSessions: parseInt(item.uniqueSessions.toString()) || 0,
        uniqueVisitors: parseInt(item.uniqueVisitors.toString()) || 0,
        pageviews: parseInt(item.pageviews.toString()) || 0,
        totalEvents: parseInt(item.totalEvents.toString()) || 0,
        uniqueUsers: parseInt(item.uniqueUsers.toString()) || 0,
        uniqueIPs: parseInt(item.uniqueIPs.toString()) || 0,
        chromeUsers: parseInt(item.chromeUsers.toString()) || 0,
        safariUsers: parseInt(item.safariUsers.toString()) || 0,
        firefoxUsers: parseInt(item.firefoxUsers.toString()) || 0,
        macOSUsers: parseInt(item.macOSUsers.toString()) || 0,
        windowsUsers: parseInt(item.windowsUsers.toString()) || 0,
        navigationEvents: parseInt(item.navigationEvents.toString()) || 0,
      }));

      // Cache for 30 minutes
      await cache.set(cacheKey, convertedResult, 1800);

      return convertedResult;
    } catch (error) {
      console.error("Analytics API error:", error);
      // Return empty array if table doesn't exist or query fails
      return [];
    }
  }
);

// API for real-time analytics events (last 24 hours)
export const RealtimeAnalyticsApi = new Api<{}, AnalyticsResponseData[]>(
  "analytics/realtime",
  async (_, { client, sql }) => {
    const cache = await MooseCache.get();
    const cacheKey = "analytics:realtime:24h";

    // Try cache first (shorter cache time for real-time data)
    const cachedData = await cache.get<AnalyticsResponseData[]>(cacheKey);
    if (cachedData && Array.isArray(cachedData)) {
      return cachedData;
    }

    const query = sql`
      SELECT 
        toString(toDate(timestamp)) as eventDate,
        appName,
        eventType,
        stage,
        toInt32(uniq(sessionId)) as uniqueSessions,
        toInt32(uniq(visitorId)) as uniqueVisitors,
        toInt32(uniq(pageviewId)) as pageviews,
        toInt32(count(*)) as totalEvents,
        toInt32(uniq(userId)) as uniqueUsers,
        toInt32(uniq(sourceIp)) as uniqueIPs,
        toInt32(countIf(browserName = 'Chrome')) as chromeUsers,
        toInt32(countIf(browserName = 'Safari')) as safariUsers,
        toInt32(countIf(browserName = 'Firefox')) as firefoxUsers,
        toInt32(countIf(operatingSystem = 'macOS')) as macOSUsers,
        toInt32(countIf(operatingSystem = 'Windows')) as windowsUsers,
        toInt32(countIf(navigationHref IS NOT NULL)) as navigationEvents
      FROM AnalyticsEvent
      WHERE toDate(timestamp) >= today() - 1
      GROUP BY toDate(timestamp), appName, eventType, stage
      ORDER BY eventDate DESC, totalEvents DESC
      LIMIT 50
    `;

    try {
      const data = await client.query.execute<AnalyticsResponseData>(query);
      const result: AnalyticsResponseData[] = await data.json();

      // Cache for 5 minutes for real-time data
      await cache.set(cacheKey, result, 300);

      return result;
    } catch (error) {
      console.error("Realtime Analytics API error:", error);
      // Return empty array if table doesn't exist or query fails
      return [];
    }
  }
);
