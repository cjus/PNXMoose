import { Api, MooseCache, joinQueries } from "@514labs/moose-lib";
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

    // Try cache first - but verify it has valid data
    const cachedData = await cache.get<HLSResponseData[]>(cacheKey);
    if (cachedData && Array.isArray(cachedData) && cachedData.length > 0) {
      // Verify cached data structure is valid (has required fields)
      const hasValidStructure = cachedData.every(
        (item) => item.eventDate && item.appName && item.eventType
      );
      if (hasValidStructure) {
        return cachedData;
      } else {
        // Cache contains invalid data, invalidate it
        await cache.delete(cacheKey);
      }
    }

    // Build WHERE filters using SQL template objects
    const filters = [sql`1`];
    if (appName) filters.push(sql`appName = ${appName}`);
    if (eventType) filters.push(sql`eventType = ${eventType}`);
    if (stage) filters.push(sql`stage = ${stage}`);
    if (startDate) filters.push(sql`toDate(timestamp) >= ${startDate}`);
    if (endDate) filters.push(sql`toDate(timestamp) <= ${endDate}`);

    const where = joinQueries({
      values: filters,
      separator: " AND ",
      prefix: "WHERE ",
    });

    const query = sql`
      SELECT 
        toString(toDate(timestamp)) as eventDate,
        appName,
        eventType,
        stage,
        toInt32(count(*)) as totalEvents,
        toInt32(uniq(userId)) as uniqueUsers,
        toInt32(ifNull(avg(bitrate), 0)) as avgBitrate,
        toInt32(ifNull(max(bitrate), 0)) as maxBitrate,
        toInt32(ifNull(min(bitrate), 0)) as minBitrate,
        toInt32(countIf(eventType = 'level-switched')) as levelSwitches,
        toInt32(countIf(eventType = 'playback-started')) as playbackStarts,
        toInt32(ifNull(avg(fragmentDuration), 0)) as avgFragmentDuration
      FROM HLSEvent
      ${where}
      GROUP BY toDate(timestamp), appName, eventType, stage
      ORDER BY eventDate DESC, totalEvents DESC
      LIMIT ${limit}
    `;

    try {
      const data = await client.query.execute<HLSResponseData>(query);
      const result: HLSResponseData[] = await data.json();

      // Debug: log result count
      console.log(`HLS API - Returning ${result.length} rows`);
      if (result.length > 0) {
        console.log(`HLS API - First row:`, JSON.stringify(result[0], null, 2));
      }

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
        toString(toDate(timestamp)) as eventDate,
        appName,
        toInt32(avg(bitrate)) as avgBitrate,
        toInt32(max(bitrate)) as maxBitrate,
        toInt32(min(bitrate)) as minBitrate,
        toInt32(countIf(eventType = 'level_switch')) as totalLevelSwitches,
        toInt32(countIf(eventType = 'playback_start')) as totalPlaybackStarts,
        toInt32(avg(fragmentDuration)) as avgFragmentDuration,
        -- Quality score calculation (higher bitrate = better quality)
        CASE 
          WHEN avg(bitrate) >= 5000000 THEN 'Excellent'
          WHEN avg(bitrate) >= 3000000 THEN 'Good'
          WHEN avg(bitrate) >= 1500000 THEN 'Fair'
          ELSE 'Poor'
        END as qualityRating
      FROM HLSEvent
      WHERE toDate(timestamp) >= today() - ${days}
      GROUP BY toDate(timestamp), appName
      ORDER BY eventDate DESC, avgBitrate DESC
    `;

    const data = await client.query.execute(query);
    const result = await data.json();

    // Cache for 15 minutes
    await cache.set(cacheKey, result, 900);

    return result;
  }
);
