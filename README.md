# 🌾 Yieldera Index Insurance Engine v2.0

> Next-generation agricultural index insurance platform for Africa

[![Python](https://img.shields.io/badge/Python-3.11+-blue.svg)](https://python.org)
[![Flask](https://img.shields.io/badge/Flask-2.3+-green.svg)](https://flask.palletsprojects.com)
[![Google Earth Engine](https://img.shields.io/badge/GEE-API-orange.svg)](https://earthengine.google.com)
[![OpenAI](https://img.shields.io/badge/OpenAI-GPT--4-purple.svg)](https://openai.com)

## 🚀 Overview

Yieldera Index Insurance Engine is a comprehensive backend platform for agricultural drought index insurance across Africa. Built from the ground up with modern architecture, it provides real-time rainfall analysis, automatic planting detection, and AI-powered risk assessment for 9 major African crops.

### ✨ Key Features

- **🌧️ Real-time Rainfall Analysis**: CHIRPS satellite data via Google Earth Engine
- **🌱 Multi-Crop Support**: 9 crops with detailed phenology phases
- **🤖 AI-Powered Summaries**: OpenAI-generated quote analysis
- **📊 Automatic Planting Detection**: Rainfall-based trigger logic
- **⚡ Fast Quote Generation**: Historical & prospective quotes in <30 seconds
- **🎯 Zone-Based Adjustments**: Agroecological risk weighting
- **💾 Field Management**: Complete field database integration
- **📈 Bulk Processing**: Multi-field quote generation

## 🏗️ Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Frontend      │    │   Flask API      │    │   Data Sources  │
│   (Your App)    │◄──►│   Quote Engine   │◄──►│   CHIRPS/GEE    │
└─────────────────┘    └──────────────────┘    │   MySQL DB      │
                                                │   OpenAI API    │
                                                └─────────────────┘
```

## 🌱 Supported Crops

| Crop | Season Days | Phases | Key Features |
|------|-------------|--------|--------------|
| **Maize** | 120 | 4 | Primary staple crop |
| **Sorghum** | 105 | 4 | Drought-tolerant cereal |
| **Soyabeans** | 115 | 4 | Protein & cash crop |
| **Cotton** | 130 | 4 | Major export crop |
| **Groundnuts** | 100 | 4 | Protein & oil crop |
| **Wheat** | 95 | 4 | Cool season cereal |
| **Barley** | 95 | 4 | Hardy cereal |
| **Millet** | 95 | 4 | Highly drought-tolerant |
| **Tobacco** | 120 | 4 | High-value cash crop |

## 🔧 Quick Start

### 1. Clone Repository
```bash
git clone https://github.com/yourusername/yieldera-backend.git
cd yieldera-backend
```

### 2. Install Dependencies
```bash
pip install -r requirements.txt
```

### 3. Set Environment Variables
```bash
cp .env.example .env
# Edit .env with your credentials
```

### 4. Run Application
```bash
python app.py
```

### 5. Test API
```bash
curl http://localhost:8080/api/health
```

## 📡 API Endpoints

### Health & System
```http
GET  /api/health              # System health check
GET  /api/crops               # List supported crops
GET  /api/config              # System configuration
```

### Quote Generation
```http
POST /api/quotes/historical   # Historical season analysis
POST /api/quotes/prospective  # Future season quotes
POST /api/quotes/field/{id}   # Field-based quotes
POST /api/quotes/bulk         # Bulk quote processing
```

### Field Management
```http
GET
