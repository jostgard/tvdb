from flask import Flask
from flask_restful import Api, Resource, reqparse
import sqlalchemy as db
import datetime

app = Flask(__name__)
api = Api(app)

launchTime = datetime.datetime.now()

#create sqlite DB
engine = db.create_engine('sqlite:///tvdb.sqlite')
connection = engine.connect()
metadata = db.MetaData()

#define table schema & create it
show = db.Table('show', metadata,
            db.Column('id', db.Integer(), primary_key=True, nullable=False),
            db.Column('title', db.String(255), nullable=False),
            db.Column('description', db.String(255), nullable=False),
            db.Column('duration', db.String(255), nullable=False),
            db.Column('originalAirDate', db.String(255), nullable=False),
            db.Column('rating', db.String(5), nullable=False), 
            db.Column('keywords', db.String(255), nullable=False),
            )
metadata.create_all(engine)

class Show(Resource):
    
    #get details of one show by id
    def get(self, id):    
        #query DB
        connection = engine.connect()
        query = db.select([show]).where(show.columns.id == id)
        rows = connection.execute(query)
        
        #convert returned rows to array of dictionaries
        results = [dict((key, value) for key, value in row.items()) for row in rows]
        
        #either return an error or the title details
        if not results: return "No results for id={}.".format(id)
        return (results)        
    
    #update a show by id
    def put(self, id):
        parser = reqparse.RequestParser()
        parser.add_argument("title")
        parser.add_argument("description")
        parser.add_argument("duration")
        parser.add_argument("originalAirDate")
        parser.add_argument("rating")
        parser.add_argument("keywords")
        args = parser.parse_args()

        #query DB
        connection = engine.connect()
        query = db.select([show]).where(show.columns.id == id)
        rows = connection.execute(query)
        
        #convert returned rows to array of dictionaries
        results = [dict((key, value) for key, value in row.items()) for row in rows]
        
        #alert if the id cannot be found
        if not results: return "No results for id={}. No update occurred.".format(id)
        
        #otherwise update values
        query = db.update(show).values(title=args["title"], description=args["description"], duration=args["duration"], originalAirDate=args["originalAirDate"], rating=args["rating"], keywords=args["keywords"])                
        query = query.where(show.columns.id == id)
        connection.execute(query)                
        return "{} (id={}) successfully updated.".format(args["title"], id), 400
        
class Shows(Resource):
    
    #get details of all shows
    def get(self):
    
        #query DB
        connection = engine.connect()
        rows = connection.execute(db.select([show])).fetchall()
        
        #convert returned rows to array of dictionaries
        results = [dict((key, value) for key, value in row.items()) for row in rows]
        if not results: return "No shows found."
        return (results)        
        
    #add a show (id will be automatically assigned)
    def post(self):    
        parser = reqparse.RequestParser()
        parser.add_argument("title")
        parser.add_argument("description")
        parser.add_argument("duration")
        parser.add_argument("originalAirDate")
        parser.add_argument("rating")
        parser.add_argument("keywords")
        args = parser.parse_args()
        
        #query DB
        connection = engine.connect()
        query = db.select([show]).where(show.columns.title == args["title"])
        rows = connection.execute(query)
        
        #convert returned rows to array of dictionaries
        results = [dict((key, value) for key, value in row.items()) for row in rows]

        #alert if title is already present (not a PK but let's assume we don't want this)
        for result in results:
            if(args["title"] == result["title"]):
                return "Show with title {} already exists. Consider updating via PUT to /show/id.".format(args["title"]), 400
        
        #otherwise insert the new values
        query = db.insert(show).values(title=args["title"], description=args["description"], duration=args["duration"], originalAirDate=args["originalAirDate"], rating=args["rating"], keywords=args["keywords"])        
        connection.execute(query)
        return "{} successfully added to TVDB".format(args["title"]), 400        
        
class Status(Resource):    
    def get(self): return "Running. Uptime: {}".format(datetime.datetime.now()-launchTime)

api.add_resource(Show, "/show/<string:id>")
api.add_resource(Shows, "/shows")
api.add_resource(Status, "/status")

app.run(debug=True)