from flask_api import FlaskAPI

# from backends.big_query import BqClient
app = FlaskAPI(__name__)
# os.environ.setdefault('FLASK_APP', 'main.py')
# os.environ.setdefault('GOOGLE_APPLICATION_CREDENTIALS', 'backends/credentials.json')


@app.route('/')
def hello():
    print("asd")
    return 'Aplicativo Flask pub-sub'
    
    
if __name__ == '__main__':
    app.run(debug=True)
