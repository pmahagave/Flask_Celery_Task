from flask import Flask, request, jsonify
import json
import pika
import threading
import requests
import time
import sqlite3
from config import Config
from models import db

app = Flask(__name__)

def get_rabbitmq_connection():
    credentials = pika.PlainCredentials(
        Config.RABBITMQ_USER,
        Config.RABBITMQ_PASSWORD
    )
    parameters = pika.ConnectionParameters(
        host=Config.RABBITMQ_HOST,
        port=Config.RABBITMQ_PORT,
        credentials=credentials
    )
    return pika.BlockingConnection(parameters)

# ============ PRODUCER API ============
@app.route('/produce', methods=['POST'])
def produce():
    try:
        data = request.get_json()
        if not data or 'item' not in data:
            return jsonify({'error': 'Missing item'}), 400
        
        item_name = data['item']
        item_id = db.insert_item(item_name)
        print(f"✅ Item inserted: {item_name} (ID: {item_id})")
        
        try:
            connection = get_rabbitmq_connection()
            channel = connection.channel()
            channel.queue_declare(queue=Config.QUEUE_NAME, durable=True)
            message = json.dumps({'item': item_name})
            channel.basic_publish(
                exchange='',
                routing_key=Config.QUEUE_NAME,
                body=message,
                properties=pika.BasicProperties(delivery_mode=2)
            )
            connection.close()
            print(f"📤 Message sent to RabbitMQ: {message}")
        except Exception as e:
            print(f"❌ RabbitMQ error: {e}")
            return jsonify({'error': f'RabbitMQ failed: {str(e)}'}), 500
        
        return jsonify({
            'message': 'Item received and queued',
            'item_id': item_id,
            'item': item_name,
            'status': 'pending'
        }), 202
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ============ STATUS API ============
@app.route('/status', methods=['GET'])
def get_status():
    try:
        conn = sqlite3.connect(Config.DATABASE_PATH)
        cursor = conn.cursor()
        status_filter = request.args.get('status')
        if status_filter:
            cursor.execute('SELECT * FROM items WHERE status = ?', (status_filter,))
        else:
            cursor.execute('SELECT * FROM items')
        items = cursor.fetchall()
        conn.close()
        return jsonify({'items': items})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ============ DELAY ENDPOINT (For Testing Threading) ============
@app.route('/delay/<int:delay_value>', methods=['GET'])
def delay_response(delay_value):
    """Simulate delay for testing threading"""
    print(f"⏳ Simulating delay of {delay_value} seconds...")
    time.sleep(delay_value)
    return jsonify({'status': 'success', 'delay': delay_value})

# ============ CONCURRENT REQUESTS API ============
@app.route('/concurrent-requests', methods=['GET'])
def concurrent_requests():
    try:
        delay_value = request.args.get('delay_value')
        if not delay_value:
            return jsonify({'error': 'Missing delay_value'}), 400
        
        try:
            delay_value = int(delay_value)
            if delay_value < 1 or delay_value > 10:
                return jsonify({'error': 'delay_value must be 1-10'}), 400
        except ValueError:
            return jsonify({'error': 'delay_value must be integer'}), 400
        
        def make_request(url, results, index):
            try:
                print(f"🔄 Thread {index} starting...")
                response = requests.get(url, timeout=30)
                results[index] = {
                    'status_code': response.status_code,
                    'success': True
                }
                print(f"✅ Thread {index} completed")
            except Exception as e:
                print(f"❌ Thread {index} error: {e}")
                results[index] = {
                    'error': str(e),
                    'success': False
                }
        
        # Local delay endpoint (instead of httpbin.org)
        base_url = f'http://localhost:5000/delay/{delay_value}'
        results = [None] * 5
        threads = []
        
        print(f"🚀 Starting 5 concurrent requests with delay {delay_value}")
        start_time = time.time()
        
        for i in range(5):
            thread = threading.Thread(
                target=make_request,
                args=(base_url, results, i)
            )
            threads.append(thread)
            thread.start()
            print(f"📤 Thread {i} started")
        
        for thread in threads:
            thread.join()
            print(f"📥 Thread joined")
        
        total_time = time.time() - start_time
        
        print(f"⏱️ Total time: {total_time:.2f} seconds")
        
        return jsonify({
            'time_taken': round(total_time, 2),
            'requests_made': 5,
            'all_successful': all(r.get('success', False) for r in results),
            'delay_value': delay_value
        }), 200
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return jsonify({'error': str(e)}), 500

# ============ CLEAR ALL ITEMS ============
@app.route('/clear', methods=['DELETE'])
def clear_items():
    try:
        conn = sqlite3.connect(Config.DATABASE_PATH)
        cursor = conn.cursor()
        cursor.execute('DELETE FROM items')
        conn.commit()
        conn.close()
        return jsonify({'message': 'All items cleared'}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    db.init_db()
    print("🚀 Flask server starting on http://localhost:5000")
    print("📌 Available endpoints:")
    print("   POST   /produce                 - Add new item")
    print("   GET    /status                  - Check all items")
    print("   GET    /delay/<int>             - Test delay")
    print("   GET    /concurrent-requests     - Test concurrent requests")
    print("   DELETE /clear                   - Clear all items")
    app.run(host='0.0.0.0', port=5000, debug=True)