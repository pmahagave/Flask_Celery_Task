from celery import Celery
from config import Config
from models import db
import json
import time

celery_app = Celery('tasks', broker=Config.CELERY_BROKER_URL)

@celery_app.task(name='process_item')
def process_item(item_data):
    try:
        if isinstance(item_data, str):
            data = json.loads(item_data)
        else:
            data = item_data
        
        item_name = data.get('item')
        if not item_name:
            return {'error': 'No item provided'}
        
        time.sleep(1)
        success = db.update_item_status(item_name, 'completed')
        
        if success:
            print(f"✅ Item '{item_name}' marked as completed")
            return {'status': 'success', 'item': item_name}
        else:
            print(f"⚠️ No pending item found: {item_name}")
            return {'status': 'failed', 'item': item_name}
            
    except Exception as e:
        print(f"❌ Error: {e}")
        return {'status': 'error', 'error': str(e)}