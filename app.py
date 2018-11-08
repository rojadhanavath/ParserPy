from flask import Flask,flash,render_template,request,redirect,Markup
from werkzeug import secure_filename
import csv
import pyodbc
import boto
import os
import sys
import json
import pyodbc
import csv
import boto.s3.connection
from boto.s3.key import Key
import pandas as pd
from pandas.io import sql
from sqlalchemy import create_engine
import datetime
import sparser
 

UPLOAD_FOLDER = 'C:\\pythonToSQL\\uploadfiles'
ALLOWED_EXTENSIONS = set(['csv','CSV','txt'])
app = Flask(__name__)

AWS_ACCESS_KEY_ID = 'Your AWS Access Key'
AWS_SECRET_ACCESS_KEY = 'Secret Key '
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.secret_key = "super secret key"
bucket_name = AWS_ACCESS_KEY_ID.lower() +'storage-Name'
conn = boto.connect_s3(AWS_ACCESS_KEY_ID,
        AWS_SECRET_ACCESS_KEY)
bucket = conn.create_bucket(bucket_name,
    location=boto.s3.connection.Location.DEFAULT)
connection_SQL = pyodbc.connect('Driver={SQL Server};'
                                'Server=server_Name;'
                                'Database=Database_Name;'
                                'uid=User_ID;pwd=Password')
cursor = connection_SQL.cursor()
	

@app.route("/")
def main():
    return render_template('Home.html')

@app.route("/fp")
def fparser():
	return render_template('SDParser.html')

@app.route("/log")
def filelog():
	cursor.execute("select * from filelog")
	mydata = cursor.fetchall()
	flash('File Upload logs','info')
	return render_template('filelog.html',data=mydata)

@app.route("/3list")
def list3():
	cursor.execute("select * from Record_Type_3")
	mydata = cursor.fetchall()
	flash('The Current Record type 3 in Database','info')
	return render_template('3list.html',data=mydata)

	

@app.route('/parser', methods = ['GET', 'POST'])
def parse_file():
	if request.method == 'POST':
		if 'file' not in request.files:
			flash('No file part','danger')
			return redirect("/")
		f = request.files['file']
		if f.filename == '':
			flash('No selected file','danger')
			return redirect("/")
		if f and allowed_file(f.filename):
			filename = secure_filename(f.filename)
			f.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
			k = Key(bucket)
			k.key = filename
			completeName = os.path.join(app.config['UPLOAD_FOLDER'], filename)  
			k.set_contents_from_filename(completeName,cb=percent_cb, num_cb=10)
			sparser.xparse(completeName)
			fquery = 'insert into filelog values ({0})'
			fquery = fquery.format(','.join('?' * 3))
			cursor.execute(fquery, [filename,datetime.datetime.today(),request.remote_addr])
			cursor.commit()
			return redirect("/log")
		else:
			flash('only csv files are allowed !','info')
			return redirect("/")
	return 'file uploaded successfully'		
 
@app.route('/uploader', methods = ['GET', 'POST'])
def upload_file():
	if request.method == 'POST':
		if 'file' not in request.files:
			flash('No file part','danger')
			return redirect("/")
		f = request.files['file']
		if f.filename == '':
			flash('No selected file','danger')
			return redirect("/")
		if f and allowed_file(f.filename):
			filename = secure_filename(f.filename)
			f.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
			k = Key(bucket)
			k.key = filename
			completeName = os.path.join(app.config['UPLOAD_FOLDER'], filename)  
			k.set_contents_from_filename(completeName,cb=percent_cb, num_cb=10)
			with open (completeName, 'r') as f:
				reader = csv.reader(f, skipinitialspace=False,delimiter=',', quoting=csv.QUOTE_NONE)
				data = next(reader)
				if len(data) != 3:
					flash('Invalid file no columns mismatch with data base.Please review','danger')
					return redirect("/")
				row_count = 0
				query = 'insert into table_A values ({0})'
				query = query.format(','.join('?' * len(data)))
				if len(data) != 0:
					cursor.execute(query, data)
					row_count = row_count + 1
				for line in reader:
					if len(line) != 0:
						if len(line) != 3:
							flash('Invalid file no columns mismatch with data base.Please review','danger')
							return redirect("/")
						cursor.execute(query, line)
						row_count = row_count + 1
			cursor.commit()
			flash(Markup(str(row_count)+' Records Inserted Succesfully! Please click <a href="/all" class="badge badge-light">here</a> to view.'),'success')
			return redirect("/")
		else:
			flash('only csv files are allowed !','info')
			return redirect("/")
	return 'file uploaded successfully'		
 
 
@app.route("/all")
def getDetails():
		cursor.execute("select * from table_A")
		mydata = cursor.fetchall()
		flash('Current Data in Database','info')
		return render_template('Details.html',data=mydata)


def percent_cb(complete, total):
    sys.stdout.write('.')
    sys.stdout.flush()

def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS
	
if __name__ == "__main__":
     app.run(debug = True)