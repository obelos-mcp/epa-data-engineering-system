from flask import Flask, send_from_directory, redirect, url_for
import os

app = Flask(__name__)

# Serve static files from website directory
@app.route('/')
def index():
    return send_from_directory('website', 'genesis.html')

@app.route('/project-overview')
def project_overview():
    return redirect(url_for('index'))

@app.route('/system-architecture')
def system_architecture():
    return send_from_directory('website/pages', 'architecture.html')

@app.route('/interactive-dashboard')
def interactive_dashboard():
    return send_from_directory('website/pages', 'dashboard.html')

# Serve static assets
@app.route('/assets/<path:filename>')
def assets(filename):
    return send_from_directory('website/assets', filename)

# Serve CSS files
@app.route('/assets/css/<path:filename>')
def css(filename):
    return send_from_directory('website/assets/css', filename)

# Serve JS files
@app.route('/assets/js/<path:filename>')
def js(filename):
    return send_from_directory('website/assets/js', filename)

# Serve pages
@app.route('/pages/<path:filename>')
def pages(filename):
    return send_from_directory('website/pages', filename)

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
