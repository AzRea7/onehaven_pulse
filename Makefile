dev:
	@echo "Starting backend and frontend..."
	(cd apps/api && source venv/Scripts/activate && uvicorn app.main:app --reload &) 
	(cd apps/web && npm run dev)

api:
	cd apps/api && source venv/Scripts/activate && uvicorn app.main:app --reload

web:
	cd apps/web && npm run dev
