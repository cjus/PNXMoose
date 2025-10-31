import { Api, MooseCache, joinQueries } from "@514labs/moose-lib";
import { tags } from "typia";

interface ErrorImpactQueryParams {
  days?: number;
  severity?: "error" | "warn" | "info";
  groupBy?: "errorClass" | "fileName" | "browser";
  appName?: string;
  stage?: "production" | "staging" | "development";
  limit?: number & tags.Type<"int32">;
}

interface BrowserBreakdown {
  browserName: string;
  count: number;
  percentage: number;
}

interface ErrorImpactResponse {
  errorClass: string;
  errorMessage: string;
  fileName?: string;
  line?: number;
  column?: number;
  occurrences: number;
  affectedUsers: number;
  affectedSessions: number;
  firstOccurrence: string;
  lastOccurrence: string;
  severity: string;
  impactScore: number;
  browserBreakdown: BrowserBreakdown[];
  urlPath?: string;
}

export const ErrorImpactApi = new Api<
  ErrorImpactQueryParams,
  ErrorImpactResponse[]
>(
  "error-impact",
  async (
    { days = 7, severity, groupBy = "errorClass", appName, stage, limit = 100 },
    { client, sql }
  ) => {
    const cache = await MooseCache.get();
    const cacheKey = `error-impact:${days}:${severity || "all"}:${groupBy}:${
      appName || "all"
    }:${stage || "all"}:${limit}`;

    const cached = await cache.get<ErrorImpactResponse[]>(cacheKey);
    if (cached && Array.isArray(cached) && cached.length > 0) {
      return cached;
    }

    // Build filters
    const filters = [sql`timestamp >= now() - INTERVAL ${days} DAY`];
    if (severity) filters.push(sql`severity = ${severity}`);
    if (appName) filters.push(sql`appName = ${appName}`);
    if (stage) filters.push(sql`stage = ${stage}`);

    const where = joinQueries({
      values: filters,
      separator: " AND ",
      prefix: "WHERE ",
    });

    // Determine grouping columns based on groupBy parameter
    let groupByColumns = "errorClass, errorMessage";
    if (groupBy === "fileName") {
      groupByColumns = "fileName, line, column";
    } else if (groupBy === "browser") {
      groupByColumns = "errorClass, errorMessage, browserName";
    }

    const query = sql`
      WITH ErrorStats AS (
        SELECT
          errorClass,
          any(errorMessage) as errorMessage,
          any(fileName) as fileName,
          any(line) as line,
          any(column) as column,
          any(severity) as severity,
          any(urlPath) as urlPath,
          count(*) as occurrences,
          uniq(userId) as affectedUsers,
          uniq(sessionId) as affectedSessions,
          min(timestamp) as firstOccurrence,
          max(timestamp) as lastOccurrence,
          (count(*) * uniq(userId)) as impactScore
        FROM ErrorEvent
        ${where}
        GROUP BY ${groupBy === "errorClass" ? sql`errorClass, errorMessage` : 
          groupBy === "fileName" ? sql`fileName, line, column` : 
          sql`errorClass, errorMessage, browserName`}
      ),
      BrowserStats AS (
        SELECT
          ${groupBy === "errorClass" ? sql`errorClass, errorMessage` : 
            groupBy === "fileName" ? sql`fileName, line, column` : 
            sql`errorClass, errorMessage, browserName`} as groupingKey,
          browserName,
          count(*) as browserCount
        FROM ErrorEvent
        ${where}
        GROUP BY groupingKey, browserName
      ),
      BrowserAgg AS (
        SELECT
          ${groupBy === "errorClass" ? sql`errorClass, errorMessage` : 
            groupBy === "fileName" ? sql`fileName, line, column` : 
            sql`errorClass, errorMessage, browserName`} as groupingKey,
          sum(browserCount) as totalForGroup,
          groupArray((browserName, browserCount)) as browserArray
        FROM BrowserStats
        GROUP BY groupingKey
      )
      SELECT
        e.errorClass ?? '' as errorClass,
        e.errorMessage ?? '' as errorMessage,
        e.fileName ?? NULL as fileName,
        if(e.line > 0, e.line, NULL) as line,
        if(e.column > 0, e.column, NULL) as column,
        toInt32(e.occurrences) as occurrences,
        toInt32(e.affectedUsers) as affectedUsers,
        toInt32(e.affectedSessions) as affectedSessions,
        toString(e.firstOccurrence) as firstOccurrence,
        toString(e.lastOccurrence) as lastOccurrence,
        e.severity ?? 'error' as severity,
        toInt64(e.impactScore) as impactScore,
        e.urlPath ?? NULL as urlPath,
        [] as browserBreakdown
      FROM ErrorStats e
      LEFT JOIN BrowserAgg b ON 
        ${groupBy === "errorClass" ? sql`e.errorClass = b.groupingKey.1 AND e.errorMessage = b.groupingKey.2` :
          groupBy === "fileName" ? sql`e.fileName = b.groupingKey.1 AND e.line = b.groupingKey.2 AND e.column = b.groupingKey.3` :
          sql`e.errorClass = b.groupingKey.1 AND e.errorMessage = b.groupingKey.2 AND e.browserName = b.groupingKey.3`}
      ORDER BY e.impactScore DESC
      LIMIT ${limit}
    `;

    try {
      const data = await client.query.execute<any>(query);
      const result: any[] = await data.json();

      // Process browser breakdown (simplified - ClickHouse array handling)
      const convertedResult = result.map((item) => {
        // For browser breakdown, we'll query separately if needed
        // For now, return empty array
        const browserBreakdown: BrowserBreakdown[] = [];

        return {
          errorClass: item.errorClass || "",
          errorMessage: item.errorMessage || "",
          fileName: item.fileName || undefined,
          line: item.line ? parseInt(item.line.toString()) : undefined,
          column: item.column ? parseInt(item.column.toString()) : undefined,
          occurrences: parseInt(item.occurrences.toString()) || 0,
          affectedUsers: parseInt(item.affectedUsers.toString()) || 0,
          affectedSessions: parseInt(item.affectedSessions.toString()) || 0,
          firstOccurrence: item.firstOccurrence || "",
          lastOccurrence: item.lastOccurrence || "",
          severity: item.severity || "error",
          impactScore: parseInt(item.impactScore.toString()) || 0,
          browserBreakdown,
          urlPath: item.urlPath || undefined,
        } as ErrorImpactResponse;
      });

      // Cache for 15 minutes
      await cache.set(cacheKey, convertedResult, 900);

      return convertedResult;
    } catch (error) {
      console.error("Error Impact API error:", error);
      return [];
    }
  }
);

