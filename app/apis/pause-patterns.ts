import { Api, MooseCache, joinQueries } from "@514labs/moose-lib";
import { tags } from "typia";

interface PausePatternQueryParams {
  videoId: string; // Required
  appName?: string;
  stage?: "production" | "staging" | "development";
  startDate?: string; // YYYY-MM-DD format
  endDate?: string; // YYYY-MM-DD format
  minPauseCount?: number;
  binSizeSec?: number;
  limit?: number & tags.Type<"int32">;
}

interface PausePatternResponse {
  videoId: string;
  pauseTimestamp: number;
  pauseCount: number;
  avgPauseDurationSec: number;
  reason: string;
  percentageOfViewers: number;
  uniqueViewers: number;
}

export const PausePatternApi = new Api<
  PausePatternQueryParams,
  PausePatternResponse[]
>(
  "video-pause-patterns",
  async (
    {
      videoId,
      appName,
      stage,
      startDate,
      endDate,
      minPauseCount = 5,
      binSizeSec = 10,
      limit = 500,
    },
    { client, sql }
  ) => {
    const cache = await MooseCache.get();
    const cacheKey = `pause-patterns:${videoId}:${appName || "all"}:${
      stage || "all"
    }:${startDate || ""}:${endDate || ""}:${minPauseCount}:${binSizeSec}:${limit}`;

    const cached = await cache.get<PausePatternResponse[]>(cacheKey);
    if (cached && Array.isArray(cached) && cached.length > 0) {
      return cached;
    }

    // Build filters
    const filters = [
      sql`reason = 'pause'`,
      sql`videoId = ${videoId}`,
      sql`videoId IS NOT NULL`,
    ];
    if (appName) filters.push(sql`appName = ${appName}`);
    if (stage) filters.push(sql`stage = ${stage}`);
    if (startDate) filters.push(sql`toDate(timestamp) >= ${startDate}`);
    if (endDate) filters.push(sql`toDate(timestamp) <= ${endDate}`);

    const pauseWhere = joinQueries({
      values: filters,
      separator: " AND ",
      prefix: "WHERE ",
    });

    const viewerFilters = [sql`videoId = ${videoId}`];
    if (appName) viewerFilters.push(sql`appName = ${appName}`);
    if (stage) viewerFilters.push(sql`stage = ${stage}`);
    if (startDate) viewerFilters.push(sql`toDate(timestamp) >= ${startDate}`);
    if (endDate) viewerFilters.push(sql`toDate(timestamp) <= ${endDate}`);

    const viewerWhere = joinQueries({
      values: viewerFilters,
      separator: " AND ",
      prefix: "WHERE ",
    });

    const query = sql`
      WITH PauseData AS (
        SELECT
          videoId,
          toInt32(floor(startPositionSec / ${binSizeSec}) * ${binSizeSec}) as pauseTimestamp,
          count(*) as pauseCount,
          avg(durationMs) / 1000 as avgPauseDurationSec,
          any(reason) as reason,
          uniq(userId) as uniqueViewers
        FROM MetricEvent
        ${pauseWhere}
        GROUP BY videoId, pauseTimestamp
        HAVING pauseCount >= ${minPauseCount}
      ),
      TotalViewers AS (
        SELECT uniq(userId) as total
        FROM MetricEvent
        ${viewerWhere}
      )
      SELECT
        p.videoId,
        toInt32(p.pauseTimestamp) as pauseTimestamp,
        toInt32(p.pauseCount) as pauseCount,
        round(p.avgPauseDurationSec, 2) as avgPauseDurationSec,
        p.reason ?? '' as reason,
        round(if(t.total > 0, p.uniqueViewers * 100.0 / t.total, 0), 2) as percentageOfViewers,
        toInt32(p.uniqueViewers) as uniqueViewers
      FROM PauseData p
      CROSS JOIN TotalViewers t
      ORDER BY p.pauseTimestamp ASC
      LIMIT ${limit}
    `;

    try {
      const data = await client.query.execute<PausePatternResponse>(query);
      const result: PausePatternResponse[] = await data.json();

      // Convert types
      const convertedResult = result.map((item) => ({
        ...item,
        pauseTimestamp: parseInt(item.pauseTimestamp.toString()) || 0,
        pauseCount: parseInt(item.pauseCount.toString()) || 0,
        avgPauseDurationSec: parseFloat(item.avgPauseDurationSec.toString()) || 0,
        percentageOfViewers: parseFloat(item.percentageOfViewers.toString()) || 0,
        uniqueViewers: parseInt(item.uniqueViewers.toString()) || 0,
      }));

      // Cache for 15 minutes
      await cache.set(cacheKey, convertedResult, 900);

      return convertedResult;
    } catch (error) {
      console.error("Pause Pattern API error:", error);
      return [];
    }
  }
);

