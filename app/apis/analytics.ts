import { Api, MooseCache } from "@514labs/moose-lib";
import { AnalyticsEventAggregatedMV } from "../views/analyticsEventAggregated";
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
    const params: any = {};

    if (appName) {
      conditions.push(`appName = {appName:String}`);
      params.appName = appName;
    }

    if (eventType) {
      conditions.push(`eventType = {eventType:String}`);
      params.eventType = eventType;
    }

    if (stage) {
      conditions.push(`stage = {stage:String}`);
      params.stage = stage;
    }

    if (startDate) {
      conditions.push(`eventDate >= {startDate:Date}`);
      params.startDate = startDate;
    }

    if (endDate) {
      conditions.push(`eventDate <= {endDate:Date}`);
      params.endDate = endDate;
    }

    const whereClause =
      conditions.length > 0 ? `WHERE ${conditions.join(" AND ")}` : "";

    const query = sql`
      SELECT 
        toString(eventDate) as eventDate,
        appName,
        eventType,
        stage,
        countMerge(totalEventsState) as totalEvents,
        uniqMerge(uniqueUsersState) as uniqueUsers,
        uniqMerge(uniqueIPsState) as uniqueIPs,
        countMerge(chromeUsersState) as chromeUsers,
        countMerge(safariUsersState) as safariUsers,
        countMerge(firefoxUsersState) as firefoxUsers,
        countMerge(macOSUsersState) as macOSUsers,
        countMerge(windowsUsersState) as windowsUsers,
        countMerge(navigationEventsState) as navigationEvents
      FROM ${AnalyticsEventAggregatedMV.targetTable}
      ${whereClause}
      ORDER BY eventDate DESC, totalEvents DESC
      LIMIT ${limit}
    `;

    const data = await client.query.execute<AnalyticsResponseData>(
      query,
      params
    );
    const result: AnalyticsResponseData[] = await data.json();

    // Cache for 30 minutes
    await cache.set(cacheKey, result, 1800);

    return result;
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
        toString(eventDate) as eventDate,
        appName,
        eventType,
        stage,
        countMerge(totalEventsState) as totalEvents,
        uniqMerge(uniqueUsersState) as uniqueUsers,
        uniqMerge(uniqueIPsState) as uniqueIPs,
        countMerge(chromeUsersState) as chromeUsers,
        countMerge(safariUsersState) as safariUsers,
        countMerge(firefoxUsersState) as firefoxUsers,
        countMerge(macOSUsersState) as macOSUsers,
        countMerge(windowsUsersState) as windowsUsers,
        countMerge(navigationEventsState) as navigationEvents
      FROM ${AnalyticsEventAggregatedMV.targetTable}
      WHERE eventDate >= today() - 1
      ORDER BY eventDate DESC, totalEvents DESC
      LIMIT 50
    `;

    const data = await client.query.execute<AnalyticsResponseData>(query);
    const result: AnalyticsResponseData[] = await data.json();

    // Cache for 5 minutes for real-time data
    await cache.set(cacheKey, result, 300);

    return result;
  }
);
