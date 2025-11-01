import { Api, MooseCache, joinQueries } from "@514labs/moose-lib";
import { tags } from "typia";

interface PlaybackSpanQueryParams {
  appName?: string;
  stage?: string;
  videoId?: string;
  sessionId?: string;
  startDate?: string; // YYYY-MM-DD
  endDate?: string; // YYYY-MM-DD
  limit?: number & tags.Type<"int32">;
}

interface PlaybackSpanRow {
  eventDate: string;
  appName: string;
  stage: string;
  videoId: string;
  sessionId: string;
  spans: number;
  avgDurationMs: number;
}

export const PlaybackSpanApi = new Api<
  PlaybackSpanQueryParams,
  PlaybackSpanRow[]
>(
  "metrics/playback-span",
  async (
    { appName, stage, videoId, sessionId, startDate, endDate, limit = 200 },
    { client, sql }
  ) => {
    const cache = await MooseCache.get();
    const cacheKey = `metrics:playback-span:${appName || "all"}:${
      stage || "all"
    }:${videoId || "all"}:${sessionId || "all"}:${startDate || ""}:${
      endDate || ""
    }:${limit}`;

    const cached = await cache.get<PlaybackSpanRow[]>(cacheKey);
    if (cached && Array.isArray(cached) && cached.length > 0) return cached;

    // Build WHERE as a single Sql object to avoid invalid parameter placement
    const filters = [sql`1`];
    if (appName) filters.push(sql`appName = ${appName}`);
    if (stage) filters.push(sql`stage = ${stage}`);
    if (videoId) filters.push(sql`videoId = ${videoId}`);
    if (sessionId) filters.push(sql`sessionId = ${sessionId}`);
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
        stage,
        videoId,
        sessionId,
        toInt32(count(*)) as spans,
        round(avg(durationMs), 2) as avgDurationMs
      FROM MetricEvent
      ${where}
      GROUP BY toDate(timestamp), appName, stage, videoId, sessionId
      ORDER BY eventDate DESC, spans DESC
      LIMIT ${limit}
    `;

    try {
      const data = await client.query.execute<PlaybackSpanRow>(query);
      const result: PlaybackSpanRow[] = await data.json();
      await cache.set(cacheKey, result, 600);
      return result;
    } catch (error) {
      console.error("Metrics API error:", error);
      // Return empty array if table doesn't exist or query fails
      return [];
    }
  }
);

interface VideoSegmentsQueryParams {
  videoId: string;
  appName?: string;
  stage?: string;
  startDate?: string; // YYYY-MM-DD
  endDate?: string; // YYYY-MM-DD
  binSizeSec?: number & tags.Type<"int32">; // default 10s bins
}

interface VideoSegmentBinRow {
  binStartSec: number;
  binEndSec: number;
  watchSec: number;
  spans: number;
}

export const VideoSegmentsApi = new Api<
  VideoSegmentsQueryParams,
  VideoSegmentBinRow[]
>(
  "metrics/video-segments",
  async (
    { videoId, appName, stage, startDate, endDate, binSizeSec = 10 },
    { client, sql }
  ) => {
    const cache = await MooseCache.get();
    const cacheKey = `metrics:video-segments:${videoId}:${appName || "all"}:${
      stage || "all"
    }:${startDate || ""}:${endDate || ""}:${binSizeSec}`;

    const cached = await cache.get<VideoSegmentBinRow[]>(cacheKey);
    if (cached && Array.isArray(cached) && cached.length > 0) return cached;

    const query = sql`
      WITH toFloat64(${binSizeSec}) AS bin
      SELECT
        toInt32(binStart) as binStartSec,
        toInt32(binStart + bin) as binEndSec,
        round(sum(watchedSec), 2) as watchSec,
        toInt32(countIf(watchedSec > 0)) as spans
      FROM (
        SELECT
          toFloat64(startPositionSec) as s,
          toFloat64(endPositionSec) as e,
          arrayJoin(
            arrayMap(i -> floor(s / bin) * bin + i * bin,
              range(toInt32(greatest(0, floor((greatest(e, s) - 1) / bin) - floor(s / bin) + 1)))
            )
          ) as binStart,
          greatest(0.0, least(greatest(e, s), binStart + bin) - greatest(s, binStart)) as watchedSec
        FROM MetricEvent
        WHERE videoId = ${videoId}
          ${appName ? sql`AND appName = ${appName}` : sql``}
          ${stage ? sql`AND stage = ${stage}` : sql``}
          ${startDate ? sql`AND toDate(timestamp) >= ${startDate}` : sql``}
          ${endDate ? sql`AND toDate(timestamp) <= ${endDate}` : sql``}
          AND durationMs > 0
          AND startPositionSec IS NOT NULL
          AND endPositionSec IS NOT NULL
      )
      GROUP BY binStart, bin
      ORDER BY binStartSec ASC
    `;

    try {
      const data = await client.query.execute<VideoSegmentBinRow>(query);
      const result: VideoSegmentBinRow[] = await data.json();

      // Cache for 15 minutes
      await cache.set(cacheKey, result, 900);
      return result;
    } catch (error) {
      console.error("VideoSegments API error:", error);
      return [];
    }
  }
);
