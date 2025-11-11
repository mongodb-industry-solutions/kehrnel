# KEHRNEL Frontend - OpenAPI Integration

This document describes the implementation of Phase 1: OpenAPI/Swagger integration that replaces static API data with dynamic schema from the FastAPI backend.

## 🎯 Implementation Overview

The frontend now dynamically fetches OpenAPI schema from your KEHRNEL backend and automatically generates:
- **API Explorer**: Interactive documentation with real endpoint information
- **Navigation Links**: Dynamic dropdown menu based on actual backend resources
- **Loading States**: Professional loading indicators while fetching data
- **Error Handling**: Graceful fallbacks when backend is unavailable

## 🚀 Quick Start

### 1. Environment Setup
```bash
# Navigate to frontend directory
cd frontend

# Copy environment file
cp .env.example .env.local

# Update API URL if your backend runs on different port
# Default: NEXT_PUBLIC_API_BASE_URL=http://localhost:9000
```

### 2. Install Dependencies
```bash
npm install
```

### 3. Start Frontend
```bash
npm run dev
```

### 4. Start Backend
Make sure your KEHRNEL FastAPI backend is running:
```bash
# Backend should be accessible at http://localhost:9000
# OpenAPI schema should be available at http://localhost:9000/openapi.json
```

## 📁 New Architecture

### Core Files Added
```
src/
├── types/
│   └── openapi.ts              # OpenAPI and application type definitions
├── services/
│   └── openapi.ts              # OpenAPI fetching and parsing service
├── hooks/
│   ├── useAPIResources.ts      # Hook for fetching API resources
│   └── useResourceNames.ts     # Hook for navigation resource names
└── components/
    └── ErrorBoundary.tsx       # Error boundary for API failures
```

### Updated Components
- **APIExplorer.tsx**: Now uses dynamic data instead of static arrays
- **Navigation.tsx**: Dynamic dropdown menu based on actual backend resources
- **layout.tsx**: Includes error boundary for better error handling

## 🔧 Configuration

### Environment Variables
- `NEXT_PUBLIC_API_BASE_URL`: Backend API base URL (default: http://localhost:9000)

### Backend Requirements
Your FastAPI backend must:
1. Expose OpenAPI schema at `/openapi.json`
2. Support CORS for frontend requests
3. Use proper OpenAPI tags to group endpoints

## 🧪 Testing the Integration

### 1. Backend Running
- ✅ **Success**: API Explorer shows real backend resources
- ✅ **Success**: Navigation dropdown shows actual endpoint categories
- ✅ **Success**: All operations display with real paths and methods

### 2. Backend Stopped
- ✅ **Success**: Shows "Failed to Load API Resources" error
- ✅ **Success**: Displays retry button
- ✅ **Success**: Shows expected backend URL
- ✅ **Success**: Navigation dropdown shows "Loading resources..."

### 3. Backend Returns Empty Schema
- ✅ **Success**: Shows "No API Resources Found" warning
- ✅ **Success**: Navigation dropdown shows "No resources available"

## 🎨 Features

### Dynamic API Explorer
- **Real-time Loading**: Fetches actual OpenAPI schema from backend
- **Smart Parsing**: Converts OpenAPI spec to existing UI structure
- **Error Recovery**: Retry functionality when backend is unavailable
- **Loading States**: Professional loading indicators

### Dynamic Navigation
- **Auto-Discovery**: Navigation links generated from actual backend resources
- **Smart Formatting**: Converts resource names to user-friendly labels
- **Fallback Handling**: Shows loading/error states appropriately

### Error Handling
- **Error Boundaries**: Catches React errors during API operations
- **Graceful Degradation**: Shows meaningful error messages
- **Recovery Options**: Retry buttons and fallback navigation

## 📊 API Resource Mapping

The system maps your FastAPI OpenAPI schema to the frontend structure:

```typescript
// FastAPI OpenAPI → Frontend Structure
{
  "paths": {
    "/aql/query": {           // Path
      "post": {               // Method
        "tags": ["AQL"],      // → Resource Name
        "summary": "...",     // → Operation Summary
        "parameters": [...],  // → Parameter definitions
        "requestBody": {...}, // → Request examples
        "responses": {...}    // → Response examples
      }
    }
  }
}
```

## 🚨 Troubleshooting

### Common Issues

**"Failed to Load API Resources"**
- Check if backend is running on correct port
- Verify CORS settings in FastAPI backend
- Ensure `/openapi.json` endpoint is accessible

**"No API Resources Found"**
- Backend OpenAPI schema might be empty
- Check if FastAPI routes have proper tags
- Verify OpenAPI documentation is enabled

**Network/CORS Errors**
- Update `NEXT_PUBLIC_API_BASE_URL` in `.env.local`
- Configure CORS in your FastAPI backend
- Check browser developer console for specific errors

### Debug Commands

```bash
# Test backend OpenAPI endpoint
curl http://localhost:9000/openapi.json

# Check frontend environment
npm run build

# Development with error details
npm run dev
```

## 🎯 Next Phase: Live API Testing

With Phase 1 complete, the next implementation phases are:

### Phase 2: Live API Testing
- Make actual HTTP requests from frontend to backend
- Real request/response examples
- Authentication handling if needed

### Phase 3: Dynamic Documentation
- Auto-generate resource documentation from OpenAPI descriptions
- Real data examples from actual API calls
- Schema validation and real-time testing

## 🔗 Integration Points

### Backend Compatibility
The frontend expects your FastAPI backend to:
- Use OpenAPI 3.0+ specification
- Provide comprehensive endpoint documentation
- Use consistent tagging for resource grouping
- Include examples in request/response schemas

### Resource Detection
Resources are automatically detected from OpenAPI tags:
- `AQL` → AQL (Archetype Query Language)
- `EHR` → EHR (Electronic Health Record)
- `Composition` → Composition
- `Contribution` → Contribution
- etc.

## 📈 Success Metrics

✅ **Phase 1 Complete When**:
- API Explorer shows real backend resources (not hardcoded data)
- Navigation dropdown reflects actual endpoint categories
- Professional loading and error states work correctly
- No static API data remains in the codebase
- Error handling works when backend is unavailable

The implementation successfully transforms the static frontend into a dynamic, backend-driven documentation system that stays synchronized with your actual API endpoints.