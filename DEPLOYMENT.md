# Yieldera Index Insurance Engine - Deployment Guide

## 🚀 Quick Start Deployment

### Prerequisites
- GitHub account
- Render.com account
- Google Cloud Platform account (for Earth Engine)
- OpenAI account (for AI summaries)
- MySQL database (cPanel or cloud)

## 📁 Project Structure

```
yieldera-backend/
├── app.py                    # Main Flask application
├── config.py                 # Configuration management
├── requirements.txt          # Python dependencies
├── Dockerfile               # Container configuration
├── core/
│   ├── __init__.py
│   ├── gee_client.py        # Google Earth Engine client
│   ├── crops.py             # Crop configurations
│   ├── database.py          # Database operations
│   ├── quote_engine.py      # Core quote logic
│   └── ai_summary.py        # AI summary generation
├── api/
│   ├── __init__.py
│   ├── quotes.py            # Quote endpoints
│   ├── fields.py            # Field management endpoints
│   └── health.py            # Health check endpoints
└── .env.example             # Environment variables template
```

## 🔧 Environment Variables

Create these environment variables in Render:

### Database Configuration
```bash
DB_HOST=your-database-host
DB_PORT=3306
DB_NAME=your-database-name
DB_USER=your-database-user
DB_PASSWORD=your-database-password
```

### Google Earth Engine
```bash
# Option 1: Service account file path (not recommended for Render)
GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json

# Option 2: Service account JSON content (recommended for Render)
GEE_SERVICE_ACCOUNT_CREDENTIALS_JSON={"type":"service_account","project_id":"..."}
```

### OpenAI Configuration
```bash
OPENAI_API_KEY=sk-your-openai-api-key
OPENAI_MODEL=gpt-4
```

### Flask Configuration
```bash
FLASK_ENV=production
SECRET_KEY=your-secret-key-change-in-production
PORT=8080
```

## 🌐 Render.com Deployment

### Step 1: Prepare Your Repository

1. **Create GitHub Repository**
   ```bash
   git init
   git add .
   git commit -m "Initial commit: Yieldera Index Insurance Engine v2.0"
   git branch -M main
   git remote add origin https://github.com/yourusername/yieldera-backend.git
   git push -u origin main
   ```

2. **Create `.env.example` file**
   ```bash
   # Database
   DB_HOST=localhost
   DB_PORT=3306
   DB_NAME=yieldera
   DB_USER=your_user
   DB_PASSWORD=your_password
   
   # Google Earth Engine
   GEE_SERVICE_ACCOUNT_CREDENTIALS_JSON={"type":"service_account"...}
   
   # OpenAI
   OPENAI_API_KEY=sk-your-key
   OPENAI_MODEL=gpt-4
   
   # Flask
   FLASK_ENV=production
   SECRET_KEY=change-this-in-production
   ```

### Step 2: Deploy to Render

1. **Connect GitHub Repository**
   - Go to [Render Dashboard](https://dashboard.render.com)
   - Click "New +" → "Web Service"
   - Connect your GitHub repository

2. **Configure Web Service**
   ```yaml
   Name: yieldera-insurance-engine
   Environment: Python 3.11
   Build Command: pip install -r requirements.txt
   Start Command: gunicorn --bind 0.0.0.0:$PORT --workers 2 --timeout 120 app:app
   ```

3. **Set Environment Variables**
   - Add all environment variables from your `.env` file
   - **Important**: Use the full JSON string for `GEE_SERVICE_ACCOUNT_CREDENTIALS_JSON`

4. **Advanced Settings**
   ```yaml
   Auto-Deploy: Yes
   Health Check Path: /api/health
   ```

## 🗄️ Database Setup

### Option 1: cPanel MySQL (Your Current Setup)
```sql
-- Ensure your existing fields table has proper indexes
CREATE INDEX idx_fields_coordinates ON fields(latitude, longitude);
CREATE INDEX idx_fields_crop_planting ON fields(crop, planting_date);
CREATE INDEX idx_fields_owner_entity ON fields(owner_entity_id);

-- The application will create additional tables automatically
```

### Option 2: Cloud Database (PlanetScale, Railway, etc.)
1. Create a MySQL database
2. Update environment variables with new connection details
3. The application will automatically create required tables

## 🔑 Google Earth Engine Setup

### Step 1: Create Service Account
1. Go to [Google Cloud Console](https://console.cloud.google.com)
2. Create new project or select existing
3. Enable Earth Engine API
4. Create service account:
   ```bash
   # Go to IAM & Admin → Service Accounts
   # Create service account with Earth Engine permissions
   ```

### Step 2: Generate Credentials
1. Download JSON key file
2. For Render deployment, copy the entire JSON content as one line:
   ```bash
   # Convert multiline JSON to single line
   cat service-account.json | jq -c . | pbcopy
   ```
3. Paste as `GEE_SERVICE_ACCOUNT_CREDENTIALS_JSON` environment variable

### Step 3: Register Earth Engine Account
```bash
# Initialize Earth Engine (one-time setup)
earthengine authenticate --service_account_file=service-account.json
```

## 🤖 OpenAI Setup

1. Get API key from [OpenAI Platform](https://platform.openai.com/api-keys)
2. Add to environment variables:
   ```bash
   OPENAI_API_KEY=sk-your-api-key
   OPENAI_MODEL=gpt-4  # or gpt-3.5-turbo for cost savings
   ```

## 🧪 Testing Your Deployment

### Step 1: Health Check
```bash
curl https://your-app.onrender.com/api/health
```

Expected response:
```json
{
  "status": "healthy",
  "service": "Yieldera Index Insurance Engine",
  "version": "2.0.0",
  "database": {"status": "connected"},
  "earth_engine": {"status": "connected"},
  "openai": {"status": "configured"}
}
```

### Step 2: Test Quote Generation
```bash
curl -X POST https://your-app.onrender.com/api/quotes/historical \
  -H "Content-Type: application/json" \
  -d '{
    "year": 2023,
    "crop": "maize",
    "expected_yield": 5.0,
    "price_per_ton": 300,
    "latitude": -17.7888,
    "longitude": 30.6015,
    "area_ha": 2.5
  }'
```

### Step 3: Test Field Integration
```bash
curl -X POST https://your-app.onrender.com/api/quotes/field/56 \
  -H "Content-Type: application/json" \
  -d '{
    "expected_yield": 5.0,
    "price_per_ton": 300,
    "year": 2024
  }'
```

## 🔄 Migration from Old System

### Step 1: Data Migration
Your existing field data will work seamlessly:
```sql
-- Verify field data compatibility
SELECT id, name, latitude, longitude, area_ha, crop 
FROM fields 
WHERE latitude IS NOT NULL AND longitude IS NOT NULL
LIMIT 5;
```

### Step 2: API Endpoint Mapping
```bash
# Old → New endpoint mapping
/api/quote_by_field_id → /api/quotes/field/{field_id}
/api/quote → /api/quotes/historical (for historical)
/api/quote → /api/quotes/prospective (for future years)
/api/crops → /api/crops
/api/health → /api/health
```

### Step 3: Update Client Applications
```javascript
// Update your frontend to use new endpoints
const BASE_URL = 'https://your-app.onrender.com/api';

// Historical quote
const response = await fetch(`${BASE_URL}/quotes/historical`, {
  method: 'POST',
  headers: {'Content-Type': 'application/json'},
  body: JSON.stringify({
    year: 2023,
    crop: 'maize',
    expected_yield: 5.0,
    price_per_ton: 300,
    latitude: -17.7888,
    longitude: 30.6015
  })
});
```

## 📊 Monitoring & Maintenance

### Render Monitoring
- Monitor deployment logs in Render dashboard
- Set up alerts for health check failures
- Monitor resource usage and scaling

### Application Monitoring
```bash
# Check application health
curl https://your-app.onrender.com/api/health/database
curl https://your-app.onrender.com/api/health/gee

# Monitor quote performance
curl https://your-app.onrender.com/api/endpoints
```

### Database Maintenance
```sql
-- Monitor quote generation
SELECT 
  DATE(created_at) as date,
  COUNT(*) as quotes_generated,
  AVG(execution_time_ms) as avg_execution_time
FROM quote_metrics 
WHERE created_at >= CURDATE() - INTERVAL 7 DAY
GROUP BY DATE(created_at);
```

## 🚨 Troubleshooting

### Common Issues

1. **Earth Engine Authentication Error**
   ```bash
   # Check if credentials are properly formatted
   echo $GEE_SERVICE_ACCOUNT_CREDENTIALS_JSON | jq .
   ```

2. **Database Connection Failed**
   ```bash
   # Verify database credentials
   mysql -h $DB_HOST -u $DB_USER -p$DB_PASSWORD $DB_NAME
   ```

3. **OpenAI API Errors**
   ```bash
   # Test API key
   curl https://api.openai.com/v1/models \
     -H "Authorization: Bearer $OPENAI_API_KEY"
   ```

### Performance Optimization

1. **Database Indexing**
   ```sql
   -- Add indexes for better performance
   CREATE INDEX idx_fields_created_at ON fields(created_at);
   CREATE INDEX idx_quotes_crop_year ON quotes(crop, year);
   ```

2. **GEE Request Optimization**
   - Batch multiple requests when possible
   - Use appropriate geometry buffer sizes
   - Monitor API quotas

## 🔐 Security Considerations

1. **Environment Variables**
   - Never commit secrets to Git
   - Use strong database passwords
   - Rotate API keys regularly

2. **Database Security**
   - Use SSL connections if available
   - Limit database user permissions
   - Regular backups

3. **API Security**
   - Consider rate limiting for production
   - Add authentication if needed
   - Monitor for unusual usage patterns

## 📈 Scaling for Production

### Horizontal Scaling
```yaml
# Render scaling configuration
instances: 2-10 (auto-scaling)
memory: 1GB per instance
cpu: 1 core per instance
```

### Database Optimization
```sql
-- For high-volume production
CREATE INDEX idx_quotes_performance ON quotes(created_at, quote_type, crop);
CREATE INDEX idx_fields_location ON fields(latitude, longitude);
```

### Caching Strategy
Consider adding Redis for:
- Frequently accessed field data
- Crop configuration caching
- GEE request caching

## 🎯 Success Metrics

Your deployment is successful when:
- ✅ Health check returns "healthy" status
- ✅ Field-based quotes work with your existing data
- ✅ Historical quotes process correctly
- ✅ Prospective quotes generate reasonable premiums
- ✅ AI summaries are generated
- ✅ Response times are under 30 seconds for single quotes

## 📞 Support

For deployment issues:
1. Check Render logs first
2. Test individual components (DB, GEE, OpenAI)
3. Verify environment variables
4. Check the `/api/health` endpoint for system status

Your new Yieldera backend is now ready for African agricultural insurance at scale! 🌍🌾
