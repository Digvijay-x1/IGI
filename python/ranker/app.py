from flask import Flask, jsonify, request

app = Flask(__name__)

# Mock Data (The "Database")
MOCK_INDEX = {
    "computer": [{"id": 1, "title": "History of Computers"}, {"id": 2, "title": "Computer Science 101"}],
    "cats": [{"id": 3, "title": "Funny Cats"}, {"id": 4, "title": "Cat Care"}]
}

@app.route('/health')
def health():
    return jsonify({"status": "healthy", "service": "ranker"})

@app.route('/search')
def search():
    query = request.args.get('q', '').lower()
    print(f"Received query: {query}")
    results = MOCK_INDEX.get(query, [])
    return jsonify({"query": query, "results": results})

if __name__ == '__main__':
    # host='0.0.0.0' is CRITICAL for Docker networking
    app.run(host='0.0.0.0', port=5000, debug=True)