import { Api, MooseCache } from "@514labs/moose-lib";
import { HLSEventAggregatedMV } from "../views/hlsEventAggregated";
import { tags } from "typia";

// Query parameters for HLS API
interface HLSQueryParams {
  appName?: string;
  eventType?: string;
  stage?: "production" | "staging" | "development";
  startDate?: string; // YYYY-MM-DD format
  endDate?: string; // YYYY-MM-DD format
  limit?: number & tags.Type<"int32">;
}

// Response data structure
interface HLSResponseData {
  eventDate: string;
  appName: string;
  eventType: string;
  stage: string;
  totalEvents: number;
  uniqueUsers: number;
  avgBitrate: number;
  maxBitrate: number;
  minBitrate: number;
  levelSwitches: number;
  playbackStarts: number;
  avgFragmentDuration: number;
}

export const HLSApi = new Api<HLSQueryParams, HLSResponseData[]>(
  "hls",
  async (
    { appName, eventType, stage, startDate, endDate, limit = 100 },
    { client, sql }
  ) => {
    const cache = await MooseCache.get();
    const cacheKey = `hls:${appName || "all"}:${eventType || "all"}:${
      stage || "all"
    }:${startDate || ""}:${endDate || ""}:${limit}`;

    // Try cache first
    const cachedData = await cache.get<HLSResponseData[]>(cacheKey);
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
        avgMerge(avgBitrateState) as avgBitrate,
        maxMerge(maxBitrateState) as maxBitrate,
        minMerge(minBitrateState) as minBitrate,
        countMerge(levelSwitchesState) as levelSwitches,
        countMerge(playbackStartsState) as playbackStarts,
        avgMerge(avgFragmentDurationState) as avgFragmentDuration
      FROM ${HLSEventAggregatedMV.targetTable}
      ${whereClause}
      ORDER BY eventDate DESC, totalEvents DESC
      LIMIT ${limit}
    `;

    try {
      const data = await client.query.execute<HLSResponseData>(query);
      const result: HLSResponseData[] = await data.json();

      // Cache for 30 minutes
      await cache.set(cacheKey, result, 1800);

      return result;
    } catch (error) {
      console.error("HLS API error:", error);
      // Return empty array if table doesn't exist or query fails
      return [];
    }
  }
);

// API for video quality metrics
export const VideoQualityApi = new Api<{ days?: number }, any[]>(
  "hls/quality",
  async ({ days = 7 }, { client, sql }) => {
    const cache = await MooseCache.get();
    const cacheKey = `hls:quality:${days}d`;

    // Try cache first
    const cachedData = await cache.get<any[]>(cacheKey);
    if (cachedData && Array.isArray(cachedData)) {
      return cachedData;
    }

    const query = sql`
      SELECT 
        toString(eventDate) as eventDate,
        appName,
        avgMerge(avgBitrateState) as avgBitrate,
        maxMerge(maxBitrateState) as maxBitrate,
        minMerge(minBitrateState) as minBitrate,
        countMerge(levelSwitchesState) as totalLevelSwitches,
        countMerge(playbackStartsState) as totalPlaybackStarts,
        avgMerge(avgFragmentDurationState) as avgFragmentDuration,
        -- Quality score calculation (higher bitrate = better quality)
        CASE 
          WHEN avgMerge(avgBitrateState) >= 5000000 THEN 'Excellent'
          WHEN avgMerge(avgBitrateState) >= 3000000 THEN 'Good'
          WHEN avgMerge(avgBitrateState) >= 1500000 THEN 'Fair'
          ELSE 'Poor'
        END as qualityRating
      FROM ${HLSEventAggregatedMV.targetTable}
      WHERE eventDate >= today() - ${days}
      GROUP BY eventDate, appName
      ORDER BY eventDate DESC, avgBitrate DESC
    `;

    const data = await client.query.execute(query);
    const result = await data.json();

    // Cache for 15 minutes
    await cache.set(cacheKey, result, 900);

    return result;
  }
);
