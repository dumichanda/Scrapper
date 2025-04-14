import os
from flask import Flask

app = Flask(__name__)

@app.route('/')
def home():
    return """
    <html>
        <head>
            <title>AdsAfrica Scraper</title>
            <style>
                body { font-family: Arial, sans-serif; margin: 40px; line-height: 1.6; }
                h1 { color: #333; }
                .container { max-width: 800px; margin: 0 auto; }
                .info { background-color: #f4f4f4; padding: 20px; border-radius: 5px; }
                .footer { margin-top: 40px; font-size: 12px; color: #777; }
            </style>
        </head>
        <body>
            <div class="container">
                <h1>AdsAfrica Scraper Status</h1>
                <div class="info">
                    <p>The scraper is configured to run via GitHub Actions:</p>
                    <ul>
                        <li>Runs automatically at 16:00 UTC daily</li>
                        <li>Runs on every push to the main branch</li>
                        <li>Saves data to Supabase database</li>
                    </ul>
                    <p>Check the GitHub Actions tab in the repository for execution details and logs.</p>
                </div>
                <div class="footer">
                    <p>This is just a status page. The actual scraper runs through GitHub Actions.</p>
                </div>
            </div>
        </body>
    </html>
    """

if __name__ == "__main__":
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)
