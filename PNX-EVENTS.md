# PNX Events - Phoenix Studios Analytics System

This Moose project now supports Phoenix Studios event ingestion and analytics with dedicated data models for analytics and HLS video streaming events.

## 🏗️ Architecture

```
PNX Apps → POST /ingest/PNXEvent → Stream Processing →
├── Analytics Events → AnalyticsEvent Table → Analytics APIs
└── HLS Events → HLSEvent Table → HLS APIs
```

## 📊 Data Models

### PNXEvent (Raw Ingestion)

Flattened structure that accepts both analytics and HLS events:

```typescript
interface PNXEvent {
  eventId: string;
  // Message fields (flattened from original nested structure)
  appName: string;
  appVersion: string;
  userId: string;
  userName: string;
  type: "analytics" | "hls";
  event: string;
  // Analytics-specific fields (optional)
  source?: string;
  href?: string;
  // HLS-specific fields (optional)
  level?: number;
  videoIndex?: number;
  bitrate?: number;
  resolution?: string;
  bandwidth?: number;
  fragmentDuration?: number;
  // Request metadata
  requestTimeEpoch: number;
  domainName: string;
  stage: "production" | "staging" | "development";
  sourceIp: string;
  userAgent: string;
}
```

## 🔄 Event Processing

### Stream Transforms

- **Smart Routing**: Events are automatically routed based on `type` field
- **User Agent Parsing**: Extracts browser and OS information
- **Caching**: Redis caching for performance (1-hour retention)
- **Error Handling**: Dead Letter Queue for failed transformations

### Processed Models

- **AnalyticsEvent**: Enriched analytics events with browser/OS detection
- **HLSEvent**: Enriched video streaming events with quality metrics

## 📈 Materialized Views

### AnalyticsEventAggregated

Daily aggregations by app, event type, and user metrics:

- Total events, unique users, unique IPs
- Browser distribution (Chrome, Safari, Firefox)
- OS distribution (macOS, Windows)
- Navigation events count

### HLSEventAggregated

Daily video streaming metrics:

- Total events, unique users
- Bitrate statistics (avg, max, min)
- Level switches and playback starts
- Fragment duration averages

## 🚀 API Endpoints

### Ingestion

```bash
POST /ingest/PNXEvent
Content-Type: application/json

{
  "eventId": "unique-id",
  "appName": "pnxplayer",
  "appVersion": "0.1.22",
  "userId": "user@example.com",
  "userName": "User Name",
  "type": "analytics",
  "event": "panel-close",
  "source": "panel-manager",
  "requestTimeEpoch": 1757800406954,
  "domainName": "api.pnxstudios.com",
  "stage": "production",
  "sourceIp": "76.149.195.194",
  "userAgent": "Mozilla/5.0..."
}
```

### Analytics APIs

```bash
# Query analytics events
GET /analytics?appName=pnxplayer&eventType=panel-close&limit=10

# Real-time analytics (last 24 hours)
GET /analytics/realtime
```

### HLS Video APIs

```bash
# Query HLS events
GET /hls?eventType=level-switched&startDate=2025-01-01&limit=10

# Video quality metrics
GET /hls/quality?days=7
```

## 🧪 Testing

Run the test script to send sample events and test APIs:

```bash
# Start Moose development server
npm run dev

# In another terminal, run the test script
node test-pnx-events.js
```

## 📝 Event Format Migration

Your original nested message format:

```json
{
  "message": {
    "appName": "pnxplayer",
    "type": "analytics",
    "event": "panel-close"
  },
  "requestTimeEpoch": 1757800406954,
  "domainName": "api.pnxstudios.com"
}
```

Is now flattened to:

```json
{
  "eventId": "unique-id",
  "appName": "pnxplayer",
  "type": "analytics",
  "event": "panel-close",
  "requestTimeEpoch": 1757800406954,
  "domainName": "api.pnxstudios.com"
}
```

## 🔧 Configuration

The system includes:

- **Redis caching** for performance optimization
- **Dead Letter Queue** for error handling
- **Materialized views** for real-time aggregations
- **Type-safe processing** with full TypeScript support

## 🚨 Fixed Issues

- ✅ **ESM Compatibility**: Fixed `pretty-ms` version conflict by downgrading to v7.0.1
- ✅ **Union Type Issues**: Flattened data structure for Moose compatibility
- ✅ **API Parameter Issues**: Fixed query execution calls
- ✅ **Type Safety**: All models properly typed with runtime validation

## 🎯 Next Steps

1. **Start the development server**: `npm run dev`
2. **Test event ingestion**: `node test-pnx-events.js`
3. **Monitor logs** for event processing
4. **Query APIs** for analytics insights
5. **Scale as needed** with production deployment

The system is now ready for Phoenix Studios event analytics! 🎉
