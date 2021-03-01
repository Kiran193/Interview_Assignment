import flask
from flask import request, jsonify
import psycopg2
import pandas as pd
import numpy as np
from flask_cors import CORS
import os
import math
import json

app = flask.Flask(__name__)
CORS(app)

def db_connect_local():
	conn = psycopg2.connect(database="db_interview_test", user="postgres", password="postgres")
	app.config["DEBUG"] = True
	return conn
    

def create_api():
    conn = db_connect_local()
    cur = conn.cursor()

    try:


        if request.form.get('type') == 'audio':

            res = json.loads(request.form.get('metadata'))
    
            query = "INSERT INTO public.song( \"ID\", \"Song_Name\", \"Duration_Sec\", \"Upload_Time\") \
            VALUES ('"+ str(res['ID']) +"', '"+ str(res['Name']) +"', '"+ str(res['Duration']) +"', '"+ str(res['Uploaded_time']) +"')"
            cur.execute(query)

            conn.commit()

            return "Audio Data created successfully"
        
        elif request.form.get('type') == 'podcast':

            res = json.loads(request.form.get('metadata'))
    
            query = "INSERT INTO public.podcast( \"ID\", \"Podcast_Name\", \"Duration_Second\", \"Upload_Time\", \"Host\", \"Participants\") \
            VALUES ('"+ str(res['ID']) +"', '"+ str(res['Name']) +"', '"+ str(res['Duration']) +"', '"+ str(res['Uploaded_time']) +"', '"+ str(res['Host']) +"','"+ str(res['Participants']).replace("[", "{").replace("]", "}").replace("'","\"") +"')"

            cur.execute(query) 
            conn.commit()

            return "Podcast Data created successfully"

        elif request.form.get('type') == 'audiobook':

            res = json.loads(request.form.get('metadata'))
    
            query = "INSERT INTO public.audiobook( \"ID\", \"Audiobook_Title\", \"Author\",\"Narrator\",\"Duration_Seconds\", \"Upload_Time\") \
            VALUES ('"+ str(res['ID']) +"', '"+ str(res['Audiobook_Title']) +"', '"+ str(res['Author']) +"', '"+ str(res['Narrator']) +"', '"+ str(res['Duration_Seconds']) +"','"+ str(res['Upload_Time']) +"')"

            cur.execute(query) 
            conn.commit()

            return "audiobook Data created successfully"

        else:

            return "Not expected audio format."
        

    except Exception as e:
        raise e

def update_api(audioFileType, audioFileID):

    conn = db_connect_local()
    cur = conn.cursor()

    try:

        if audioFileType == 'audio':

            res = json.loads(request.form.get('metadata'))
    
            query = "UPDATE public.song SET \"ID\"='"+ str(res['ID']) +"', \"Song_Name\"='"+ str(res['Name']) +"', \"Duration_Sec\"='"+ str(res['Duration']) +"', \"Upload_Time\"='"+ str(res['Uploaded_time']) +"' WHERE \"ID\" = '" + str(audioFileID)+ "'"

            cur.execute(query)
            conn.commit()

            return "Song updated"
        
        elif audioFileType == 'audiobook':
            
            res = json.loads(request.form.get('metadata'))
    
            query = "UPDATE public.audiobook SET \
                \"ID\"='"+ str(res['ID']) +"', \
                \"Audiobook_Title\"='"+ str(res['Audiobook_Title']) +"', \
                \"Author\"='"+ str(res['Author']) +"', \
                \"Narrator\"='"+ str(res['Narrator']) +"', \
                \"Duration_Seconds\"='"+ str(res['Duration_Seconds']) +"', \
                \"Upload_Time\"='"+ str(res['Upload_Time']) +"' \
                 WHERE \"ID\" = '" + str(audioFileID)+ "'"

            cur.execute(query)
            conn.commit()

            return "Audiobook updated"

        elif audioFileType == 'podcast':
            
            res = json.loads(request.form.get('metadata'))
    
            query = "UPDATE public.podcast SET \
                \"ID\"='"+ str(res['ID']) +"', \
                \"Podcast_Name\"='"+ str(res['Name']) +"', \
                \"Duration_Second\"='"+ str(res['Duration']) +"', \
                \"Upload_Time\"='"+ str(res['Uploaded_time']) +"', \
                \"Host\"='"+ str(res['Host']) +"', \
                \"Participants\"='"+ str(res['Participants']).replace("[", "{").replace("]", "}").replace("'","\"") +"' \
                 WHERE \"ID\" = '" + str(audioFileID)+ "'"

            cur.execute(query)
            conn.commit()

            return "Podcast updated"

        else:

            return "Not expected audio format."
    
    except Exception as e:
        raise e

def delete_api(audioFileType, audioFileID):

    conn = db_connect_local()
    cur = conn.cursor()

    try:

        if audioFileType == 'audio':

            query = "DELETE FROM public.song WHERE \"ID\" =  '"+ str(audioFileID) +"';"

            cur.execute(query)
            conn.commit()

            return "Deleted row with ID" + str(audioFileID)
        
        elif audioFileType == 'podcast':

            query = "DELETE FROM public.podcast WHERE \"ID\" =  '"+ str(audioFileID) +"';"

            cur.execute(query)
            conn.commit()

            return "Deleted row with ID" + str(audioFileID)

        elif audioFileType == 'audiobook':

            query = "DELETE FROM public.audiobook WHERE \"ID\" =  '"+ str(audioFileID) +"';"

            cur.execute(query)
            conn.commit()

            return "Deleted row with ID" + str(audioFileID)

        else:

            return "Not expected audio format." 

    except Exception as e:
         raise e   

def get_api(audioFileType, audioFileID):

    conn = db_connect_local()
    cur = conn.cursor()

    if (audioFileType == 'audio') and (audioFileID != ""):

        query = "SELECT * FROM public.song WHERE \"ID\" = "+ str(audioFileID)
        cur.execute(query)

        # conn.commit()
        rows = cur.fetchall()
        conn.close()
        # return jsonify(rows)
        print("Data print with ID")
        return jsonify(rows)

    elif (audioFileType == 'audio') and (audioFileID == ""):
        query = "SELECT * FROM public.song"
        cur.execute(query)

        # conn.commit()
        rows = cur.fetchall()
        conn.close()
        print("All Data print")
        return jsonify(rows)

    if (audioFileType == 'audiobook') and (audioFileID != ""):

        query = "SELECT * FROM public.audiobook WHERE \"ID\" = "+ str(audioFileID)
        cur.execute(query)

        # conn.commit()
        rows = cur.fetchall()
        conn.close()
        # return jsonify(rows)
        return jsonify(rows)

    elif (audioFileType == 'audiobook') and (audioFileID == ""):
        query = "SELECT * FROM public.audiobook"
        cur.execute(query)

        # conn.commit()
        rows = cur.fetchall()
        conn.close()
        return jsonify(rows)

    if (audioFileType == 'podcast') and (audioFileID != ""):

        query = "SELECT * FROM public.podcast WHERE \"ID\" = "+ str(audioFileID)
        cur.execute(query)

        # conn.commit()
        rows = cur.fetchall()
        conn.close()
        # return jsonify(rows)
        return jsonify(rows)

    elif (audioFileType == 'podcast') and (audioFileID == ""):
        query = "SELECT * FROM public.podcast"
        cur.execute(query)

        # conn.commit()
        rows = cur.fetchall()
        conn.close()
        return jsonify(rows)


@app.route('/', methods=['GET']) 
def Test_api():
    """
    """
    
    return "Done"

@app.route('/create', methods=['POST']) 
def create():
    """
    The request will have the following fields:
    - audioFileType – mandatory, one of the 3 audio types possible
    - audioFileMetadata – mandatory, dictionary, contains the metadata for one
    of the three audio files (song, podcast, audiobook)
    """
    
    return create_api()

@app.route('/update/<audioFileType>/<audioFileID>', methods=['POST']) 
def update(audioFileType, audioFileID):
    """
    - The route be in the following format: “<audioFileType>/<audioFileID>”
    - The request body will be the same as the upload
    """
   
    return update_api(audioFileType, audioFileID)

@app.route('/delete/<audioFileType>/<audioFileID>', methods=['POST']) 
def delete(audioFileType, audioFileID):
    """
    - The route will be in the following format:
        “<audioFileType>/<audioFileID>”
    """
    
    return delete_api(audioFileType, audioFileID)

@app.route('/get/<audioFileType>', defaults={'audioFileID': ""}, methods=['POST']) 
@app.route('/get/<audioFileType>/<audioFileID>', methods=['POST']) 
def get(audioFileType, audioFileID):
    """ 
    - The route “<audioFileType>/<audioFileID>” will return the specific audio file
    - The route “<audioFileType>” will return all the audio files of that type
    """
    
    return get_api(audioFileType, audioFileID)


if __name__=="__main__":
	app.debug = True
	app.run(host='127.0.0.1', port=8081)