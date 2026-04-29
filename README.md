# Real Estate Cycle Platform

## Setup

### Frontend
cd apps/web
npm install
npm run dev

### Backend
cd apps/api
source venv/Scripts/activate
uvicorn app.main:app --reload

## Structure

apps/web        - Next.js frontend  
apps/api        - FastAPI backend  
pipelines/      - ETL jobs  
dbt/            - transformations  
infra/          - deployment configs  
