// Quick test to verify API client functionality
// This can be run in the browser console for testing

console.log('🧪 Testing KEHRNEL API Client...');

// Test fetching OpenAPI spec
fetch('http://localhost:9000/openapi.json')
  .then(response => response.json())
  .then(data => {
    console.log('✅ OpenAPI Spec loaded:', data.info.title);
    console.log('📋 Available endpoints:', Object.keys(data.paths).length);
  })
  .catch(error => {
    console.error('❌ Failed to load OpenAPI spec:', error);
  });

// Test a simple GET request
fetch('http://localhost:9000/')
  .then(response => response.json())
  .then(data => {
    console.log('✅ Backend root endpoint accessible:', data);
  })
  .catch(error => {
    console.error('❌ Backend not accessible:', error);
  });

console.log('🔄 API tests running...');