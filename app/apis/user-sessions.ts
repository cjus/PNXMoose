import { Api, MooseCache, joinQueries } from "@514labs/moose-lib";
import { tags } from "typia";

interface UserSessionQueryParams {
  userId?: string;
  appName?: string;
  stage?: "production" | "staging" | "development";
  startDate?: string; // YYYY-MM-DD format
  endDate?: string; // YYYY-MM-DD format
  minDurationMinutes?: number;
  limit?: number & tags.Type<"int32">;
}

interface UserSessionResponse {
  userId: string;
  sessionCount: number;
  activeDays: number;
  totalEvents: number;
  avgSessionDurationMinutes: number;
  lastSeenDate: string;
  firstSeenDate: string;
  daysSinceFirstVisit: number;
  videoIds: string[];
  primaryBrowser: string;
  primaryOS: string;
  totalWatchTimeMinutes: number;
  errorCount: number;
}

export const UserSessionApi = new Api<
  UserSessionQueryParams,
  UserSessionResponse[]
>(
  "user-sessions",
  async (
    {
      userId,
      appName,
      stage,
      startDate,
      endDate,
      minDurationMinutes = 0,
      limit = 100,
    },
    { client, sql }
  ) => {
    // Ensure numeric parameters have proper defaults
    const minDuration = minDurationMinutes ?? 0;
    const limitValue = limit ?? 100;

    const cache = await MooseCache.get();
    const cacheKey = `user-sessions:${userId || "all"}:${appName || "all"}:${
      stage || "all"
    }:${startDate || ""}:${endDate || ""}:${minDuration}:${limitValue}`;

    const cached = await cache.get<UserSessionResponse[]>(cacheKey);
    if (cached && Array.isArray(cached) && cached.length > 0) {
      return cached;
    }

    // Build filters using joinQueries pattern
    const filters = [sql`1`];
    if (userId) filters.push(sql`userId = ${userId}`);
    if (startDate) filters.push(sql`toDate(timestamp) >= ${startDate}`);
    if (endDate) filters.push(sql`toDate(timestamp) <= ${endDate}`);
    if (appName) filters.push(sql`appName = ${appName}`);
    if (stage) filters.push(sql`stage = ${stage}`);

    const where = joinQueries({
      values: filters,
      separator: " AND ",
      prefix: "WHERE ",
    });

    const query = sql`
      WITH CombinedEvents AS (
        SELECT sessionId, userId, timestamp, eventType, videoId, browserName, operatingSystem, 'analytics' as source
        FROM AnalyticsEvent
        ${where}
          AND userId IS NOT NULL
        UNION ALL
        SELECT sessionId, userId, timestamp, eventType, videoId, browserName, operatingSystem, 'metric' as source
        FROM MetricEvent
        ${where}
          AND userId IS NOT NULL
        UNION ALL
        SELECT sessionId, userId, timestamp, eventType, NULL as videoId, browserName, operatingSystem, 'error' as source
        FROM ErrorEvent
        ${where}
          AND userId IS NOT NULL
      ),
      SessionDurations AS (
        SELECT
          userId,
          sessionId,
          min(timestamp) as sessionStart,
          max(timestamp) as sessionEnd,
          dateDiff('minute', min(timestamp), max(timestamp)) as sessionDurationMinutes
        FROM CombinedEvents
        GROUP BY userId, sessionId
      ),
      UserMetrics AS (
        SELECT
          c.userId,
          uniq(c.sessionId) as sessionCount,
          uniq(toDate(c.timestamp)) as activeDays,
          count(*) as totalEvents,
          avg(sd.sessionDurationMinutes) as avgSessionDurationMinutes,
          max(toDate(c.timestamp)) as lastSeenDate,
          min(toDate(c.timestamp)) as firstSeenDate,
          dateDiff('day', min(c.timestamp), max(c.timestamp)) as daysSinceFirstVisit,
          groupUniqArray(c.videoId) as videoIds
        FROM CombinedEvents c
        LEFT JOIN SessionDurations sd ON c.userId = sd.userId AND c.sessionId = sd.sessionId
        GROUP BY c.userId
        HAVING avgSessionDurationMinutes >= ${minDuration}
      ),
      BrowserOS AS (
        SELECT
          userId,
          argMax(browserName, browserCount) as primaryBrowser,
          argMax(operatingSystem, osCount) as primaryOS
        FROM (
          SELECT
            userId,
            browserName,
            operatingSystem,
            count(*) as browserCount,
            count(*) as osCount
          FROM CombinedEvents
          WHERE browserName IS NOT NULL OR operatingSystem IS NOT NULL
          GROUP BY userId, browserName, operatingSystem
        )
        GROUP BY userId
      ),
      WatchTime AS (
        SELECT
          userId,
          sum(durationMs) / 1000 / 60 as totalWatchTimeMinutes
        FROM MetricEvent
        ${where}
          AND userId IS NOT NULL
        GROUP BY userId
      ),
      ErrorCounts AS (
        SELECT
          userId,
          count(*) as errorCount
        FROM ErrorEvent
        ${where}
          AND userId IS NOT NULL
        GROUP BY userId
      )
      SELECT
        u.userId,
        toInt32(u.sessionCount) as sessionCount,
        toInt32(u.activeDays) as activeDays,
        toInt32(u.totalEvents) as totalEvents,
        round(u.avgSessionDurationMinutes, 2) as avgSessionDurationMinutes,
        toString(u.lastSeenDate) as lastSeenDate,
        toString(u.firstSeenDate) as firstSeenDate,
        toInt32(u.daysSinceFirstVisit) as daysSinceFirstVisit,
        arrayFilter(x -> x IS NOT NULL, u.videoIds) as videoIds,
        b.primaryBrowser ?? '' as primaryBrowser,
        b.primaryOS ?? '' as primaryOS,
        round(ifNull(w.totalWatchTimeMinutes, 0), 2) as totalWatchTimeMinutes,
        toInt32(ifNull(e.errorCount, 0)) as errorCount
      FROM UserMetrics u
      LEFT JOIN BrowserOS b ON u.userId = b.userId
      LEFT JOIN WatchTime w ON u.userId = w.userId
      LEFT JOIN ErrorCounts e ON u.userId = e.userId
      ORDER BY u.sessionCount DESC, u.totalEvents DESC
      LIMIT ${limitValue}
    `;

    try {
      const data = await client.query.execute<UserSessionResponse>(query);
      const result: UserSessionResponse[] = await data.json();

      // Convert types
      const convertedResult = result.map((item) => ({
        ...item,
        sessionCount: parseInt(item.sessionCount.toString()) || 0,
        activeDays: parseInt(item.activeDays.toString()) || 0,
        totalEvents: parseInt(item.totalEvents.toString()) || 0,
        avgSessionDurationMinutes:
          parseFloat(item.avgSessionDurationMinutes.toString()) || 0,
        daysSinceFirstVisit: parseInt(item.daysSinceFirstVisit.toString()) || 0,
        totalWatchTimeMinutes:
          parseFloat(item.totalWatchTimeMinutes.toString()) || 0,
        errorCount: parseInt(item.errorCount.toString()) || 0,
        videoIds: Array.isArray(item.videoIds) ? item.videoIds : [],
      }));

      // Cache for 15 minutes
      await cache.set(cacheKey, convertedResult, 900);

      return convertedResult;
    } catch (error) {
      console.error("User Session API error:", error);
      return [];
    }
  }
);
