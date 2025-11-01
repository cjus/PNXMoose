import { Api, MooseCache, joinQueries } from "@514labs/moose-lib";
import { tags } from "typia";

interface VideoEngagementQueryParams {
  videoId?: string;
  appName?: string;
  stage?: "production" | "staging" | "development";
  startDate?: string; // YYYY-MM-DD format
  endDate?: string; // YYYY-MM-DD format
  limit?: number & tags.Type<"int32">;
}

interface VideoEngagementResponse {
  videoId: string;
  totalViews: number;
  uniqueViewers: number;
  totalWatchTimeMinutes: number;
  avgCompletionRate: number;
  completedViews: number;
  earlyDropoffs: number;
  avgWatchTimePerSession: number;
  returnViewerRate: number;
  avgPauseCount: number;
  firstViewDate: string;
  lastViewDate: string;
}

export const VideoEngagementApi = new Api<
  VideoEngagementQueryParams,
  VideoEngagementResponse[]
>(
  "video-engagement",
  async (
    { videoId, appName, stage, startDate, endDate, limit = 100 },
    { client, sql }
  ) => {
    const cache = await MooseCache.get();
    const cacheKey = `video-engagement:${videoId || "all"}:${
      appName || "all"
    }:${stage || "all"}:${startDate || ""}:${endDate || ""}:${limit}`;

    const cached = await cache.get<VideoEngagementResponse[]>(cacheKey);
    if (cached && Array.isArray(cached) && cached.length > 0) {
      // Verify cached data has videoId fields - if not, re-fetch (cache invalidation)
      const hasVideoIds = cached.every((item) => item.videoId);
      if (hasVideoIds) {
        return cached;
      } else {
        // Cache contains old format without videoId, invalidate it
        await cache.delete(cacheKey);
      }
    }

    // Build WHERE filters
    // Require videoId and durationMs for meaningful engagement metrics
    // totalDurationSec can be NULL, but if present should be > 0 for completion calculations
    const filters = [
      sql`videoId IS NOT NULL`,
      sql`videoId != ''`,
      sql`durationMs > 0`,
    ];
    // Only require totalDurationSec > 0 if we're calculating completion rates
    // For now, allow NULL totalDurationSec to capture more data
    if (videoId) filters.push(sql`videoId = ${videoId}`);
    if (appName) filters.push(sql`appName = ${appName}`);
    if (stage) filters.push(sql`stage = ${stage}`);
    if (startDate) filters.push(sql`toDate(timestamp) >= ${startDate}`);
    if (endDate) filters.push(sql`toDate(timestamp) <= ${endDate}`);

    const where = joinQueries({
      values: filters,
      separator: " AND ",
      prefix: "WHERE ",
    });

    const query = sql`
      WITH VideoMetrics AS (
        SELECT
          videoId,
          count(*) as totalViews,
          uniq(userId) as uniqueViewers,
          sum(durationMs) / 1000 / 60 as totalWatchTimeMinutes,
          avg(endPositionSec / nullIf(totalDurationSec, 0) * 100) as avgCompletionRate,
          countIf(endPositionSec >= totalDurationSec * 0.95) as completedViews,
          countIf(endPositionSec < totalDurationSec * 0.25) as earlyDropoffs,
          uniq(sessionId) as uniqueSessions,
          min(toDate(timestamp)) as firstViewDate,
          max(toDate(timestamp)) as lastViewDate
        FROM MetricEvent
        ${where}
        GROUP BY videoId
      ),
      PauseCounts AS (
        SELECT
          videoId,
          count(*) as totalPauses,
          uniq(sessionId) as sessionsWithPauses
        FROM MetricEvent
        WHERE reason = 'pause'
          AND videoId IS NOT NULL
          ${videoId ? sql`AND videoId = ${videoId}` : sql``}
          ${appName ? sql`AND appName = ${appName}` : sql``}
          ${stage ? sql`AND stage = ${stage}` : sql``}
          ${startDate ? sql`AND toDate(timestamp) >= ${startDate}` : sql``}
          ${endDate ? sql`AND toDate(timestamp) <= ${endDate}` : sql``}
        GROUP BY videoId
      ),
      ReturnViewers AS (
        SELECT
          videoId,
          userId
        FROM MetricEvent
        WHERE videoId IS NOT NULL
          ${videoId ? sql`AND videoId = ${videoId}` : sql``}
          ${appName ? sql`AND appName = ${appName}` : sql``}
          ${stage ? sql`AND stage = ${stage}` : sql``}
          ${startDate ? sql`AND toDate(timestamp) >= ${startDate}` : sql``}
          ${endDate ? sql`AND toDate(timestamp) <= ${endDate}` : sql``}
        GROUP BY videoId, userId
        HAVING count(*) > 1
      )
      SELECT
        v.videoId as videoId,
        toInt32(v.totalViews) as totalViews,
        toInt32(v.uniqueViewers) as uniqueViewers,
        round(v.totalWatchTimeMinutes, 2) as totalWatchTimeMinutes,
        round(v.avgCompletionRate, 2) as avgCompletionRate,
        toInt32(v.completedViews) as completedViews,
        toInt32(v.earlyDropoffs) as earlyDropoffs,
        round(v.totalWatchTimeMinutes / nullIf(v.uniqueSessions, 0), 2) as avgWatchTimePerSession,
        round(if(r.returnViewers > 0, r.returnViewers * 100.0 / v.uniqueViewers, 0), 2) as returnViewerRate,
        round(if(p.totalPauses > 0, p.totalPauses / nullIf(v.uniqueSessions, 0), 0), 2) as avgPauseCount,
        toString(v.firstViewDate) as firstViewDate,
        toString(v.lastViewDate) as lastViewDate
      FROM VideoMetrics v
      LEFT JOIN PauseCounts p ON v.videoId = p.videoId
      LEFT JOIN (
        SELECT videoId, count(DISTINCT userId) as returnViewers
        FROM ReturnViewers
        GROUP BY videoId
      ) r ON v.videoId = r.videoId
      ORDER BY v.totalWatchTimeMinutes DESC
      LIMIT ${limit}
    `;

    try {
      const data = await client.query.execute<VideoEngagementResponse>(query);
      const result: VideoEngagementResponse[] = await data.json();

      // Debug: log first result to check videoId presence
      if (result.length > 0) {
        console.log(
          "Video Engagement API - First result:",
          JSON.stringify(result[0], null, 2)
        );
      }

      // Convert string values to numbers
      const convertedResult = result.map((item) => ({
        ...item,
        videoId: String(item.videoId || ""),
        totalViews: parseInt(item.totalViews.toString()) || 0,
        uniqueViewers: parseInt(item.uniqueViewers.toString()) || 0,
        totalWatchTimeMinutes:
          parseFloat(item.totalWatchTimeMinutes.toString()) || 0,
        avgCompletionRate: parseFloat(item.avgCompletionRate.toString()) || 0,
        completedViews: parseInt(item.completedViews.toString()) || 0,
        earlyDropoffs: parseInt(item.earlyDropoffs.toString()) || 0,
        avgWatchTimePerSession:
          parseFloat(item.avgWatchTimePerSession.toString()) || 0,
        returnViewerRate: parseFloat(item.returnViewerRate.toString()) || 0,
        avgPauseCount: parseFloat(item.avgPauseCount.toString()) || 0,
      }));

      // Debug: log first converted result
      if (convertedResult.length > 0) {
        console.log(
          "Video Engagement API - First converted result:",
          JSON.stringify(convertedResult[0], null, 2)
        );
      }

      // Cache for 30 minutes
      await cache.set(cacheKey, convertedResult, 1800);

      return convertedResult;
    } catch (error) {
      console.error("Video Engagement API error:", error);
      // Log more details for debugging
      if (error instanceof Error) {
        console.error("Error message:", error.message);
        console.error("Error stack:", error.stack);
      }
      return [];
    }
  }
);
