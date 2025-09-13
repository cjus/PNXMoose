#!/usr/bin/env node

/**
 * Test script to send PNX events to the Moose ingestion endpoint
 * Usage: node test-pnx-events.js
 */

const { v4: uuidv4 } = require('uuid');

// Sample analytics event (flattened structure)
const analyticsEvent = {
  eventId: uuidv4(),
  // Message fields (flattened)
  appName: "pnxplayer",
  appVersion: "0.1.22",
  userId: "cjus@ieee.org",
  userName: "Carlos Justiniano",
  type: "analytics",
  event: "panel-close",
  source: "panel-manager", // Analytics-specific field
  // Request metadata
  requestTimeEpoch: Date.now(),
  domainName: "api.pnxstudios.com",
  stage: "production",
  sourceIp: "76.149.195.194",
  userAgent: "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36"
};

// Sample HLS event (flattened structure)
const hlsEvent = {
  eventId: uuidv4(),
  // Message fields (flattened)
  appName: "pnxplayer",
  appVersion: "0.1.22",
  userId: "cjus@ieee.org",
  userName: "Carlos Justiniano",
  type: "hls",
  event: "level-switched",
  // HLS-specific fields
  level: 2,
  videoIndex: 2,
  bitrate: 6000000,
  resolution: "1920x1080",
  bandwidth: 6000000,
  // Request metadata
  requestTimeEpoch: Date.now(),
  domainName: "api.pnxstudios.com",
  stage: "production",
  sourceIp: "76.149.195.194",
  userAgent: "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36"
};

async function sendEvent(event, eventType) {
  try {
    console.log(`\n🚀 Sending ${eventType} event...`);
    console.log(`Event ID: ${event.eventId}`);
    console.log(`App: ${event.appName} v${event.appVersion}`);
    console.log(`User: ${event.userName}`);
    console.log(`Type: ${event.type}`);
    console.log(`Event: ${event.event}`);

    const response = await fetch('http://localhost:4000/ingest/PNXEvent', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(event),
    });

    if (response.ok) {
      console.log(`✅ ${eventType} event sent successfully!`);
    } else {
      console.error(`❌ Failed to send ${eventType} event:`, response.status, response.statusText);
    }
  } catch (error) {
    console.error(`❌ Error sending ${eventType} event:`, error.message);
  }
}

async function testAPIs() {
  console.log('\n📊 Testing Analytics API...');
  try {
    const analyticsResponse = await fetch('http://localhost:4000/analytics?limit=5');
    if (analyticsResponse.ok) {
      const analyticsData = await analyticsResponse.json();
      console.log('✅ Analytics API response:', analyticsData);
    } else {
      console.log('⚠️  Analytics API not ready yet (expected if no data)');
    }
  } catch (error) {
    console.log('⚠️  Analytics API error:', error.message);
  }

  console.log('\n🎬 Testing HLS API...');
  try {
    const hlsResponse = await fetch('http://localhost:4000/hls?limit=5');
    if (hlsResponse.ok) {
      const hlsData = await hlsResponse.json();
      console.log('✅ HLS API response:', hlsData);
    } else {
      console.log('⚠️  HLS API not ready yet (expected if no data)');
    }
  } catch (error) {
    console.log('⚠️  HLS API error:', error.message);
  }
}

async function main() {
  console.log('🧪 PNX Event Testing Script');
  console.log('============================');
  
  // Check if Moose server is running
  try {
    const healthResponse = await fetch('http://localhost:4000/health');
    if (!healthResponse.ok) {
      throw new Error('Health check failed');
    }
    console.log('✅ Moose server is running');
  } catch (error) {
    console.error('❌ Moose server is not running. Please start it with: npm run dev');
    process.exit(1);
  }

  // Send test events
  await sendEvent(analyticsEvent, 'Analytics');
  await sendEvent(hlsEvent, 'HLS');

  // Wait a moment for processing
  console.log('\n⏳ Waiting 3 seconds for event processing...');
  await new Promise(resolve => setTimeout(resolve, 3000));

  // Test APIs
  await testAPIs();

  console.log('\n🎉 Test completed!');
  console.log('\n📚 Available endpoints:');
  console.log('  POST /ingest/PNXEvent - Ingest events');
  console.log('  GET  /analytics - Query analytics events');
  console.log('  GET  /analytics/realtime - Real-time analytics');
  console.log('  GET  /hls - Query HLS video events');
  console.log('  GET  /hls/quality - Video quality metrics');
}

// Add uuid dependency check
try {
  require('uuid');
} catch (error) {
  console.error('❌ Missing uuid dependency. Install with: npm install uuid');
  process.exit(1);
}

main().catch(console.error);
