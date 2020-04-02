import numpy as np
import pandas as pd
import datetime as dt

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func

from flask import Flask, jsonify

#################################################
# Database Setup
#################################################
engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(engine, reflect=True)

# Save reference to the table
Measurement = Base.classes.measurement
Station = Base.classes.station

#################################################
# Flask Setup
#################################################
app = Flask(__name__)


#################################################
# Flask Routes
#################################################

@app.route("/")
def welcome():
    """List all available api routes."""
    return (
        "Available Routes:<br/>"
        "/api/v1.0/precipitation<br/>"
        "-------- Returns JSON object with 'date' as key and 'precipitation' as value.<br/><br/>"
        "/api/v1.0/stations<br/>"
        "-------- Returns JSON array of stations.<br/><br/>"
        "/api/v1.0/tobs<br/>"
        "-------- Return a JSON array of Temperature Observations (tobs) for the previous year. <br/><br/>"
        "/api/v1.0/{start_date}<br/>"
        "-------- Return a JSON array of the minimum temperature, the average temperature, and the max temperature for a date range beginning at the given start date, ending at the end of records.<br/><br/>"
        "/api/v1.0/{start_date}/{end_date}<br/>"
        "-------- Return a JSON array of the minimum temperature, the average temperature, and the max temperature for a date range beginning at the given start date, ending at the given end date.<br/><br/>"
    )


@app.route("/api/v1.0/precipitation")
def precipitation():
    # Create our session (link) from Python to the DB
    session = Session(engine)

    """Return Measurements"""
    # Query all Measurements
    results = session.query(Measurement.date, Measurement.prcp).order_by(Measurement.date.asc()).all()

    session.close()

    measurement_date = [result[0] for result in results]
    measurement_prcp = [result[1] for result in results]
    precipitation_dict = dict(zip(measurement_date, measurement_prcp))

    return jsonify(precipitation_dict)

@app.route("/api/v1.0/stations")
def stations():
    # Create our session (link) from Python to the DB
    session = Session(engine)
    """Return Measurements"""
    results = session.query(Measurement.station).all()

    session.close()

    measurement_stat = [result[0] for result in results]
    measurement_stat_unique = np.unique(measurement_stat).tolist()

    return jsonify(measurement_stat_unique)

@app.route("/api/v1.0/tobs")
def tobs():
    # Create our session (link) from Python to the DB
    session = Session(engine)

    data = engine.execute("SELECT MAX(DATE) FROM measurement")

    for record in data:
        max_date = record[0]
        max_date = dt.date.fromisoformat(max_date)
        print(max_date)
    
    min_date = max_date - dt.timedelta(days=365)
    
    results = session.query(Measurement.date, Measurement.tobs).order_by(Measurement.date.asc()).all()
    session.close()

    measurement_date = [result[0] for result in results]
    measurement_tobs = [result[1] for result in results]
    
    measurement_date_formatted = [dt.date.fromisoformat(date) for date in measurement_date]
    
    df = pd.DataFrame({
        "date" : measurement_date_formatted,
        "tobs" : measurement_tobs
    })

    tobs_df = df.loc[df.date > min_date]
    tobs_df = tobs_df.tobs.to_list()

    return jsonify(tobs_df)
    
@app.route("/api/v1.0/<start>")
def start(start):
    # Create our session (link) from Python to the DB
    session = Session(engine)

    data = engine.execute("SELECT MAX(DATE) FROM measurement")

    for record in data:
        max_date = record[0]
        max_date = dt.date.fromisoformat(max_date)
    
    tmam = session.query(func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)).\
        filter(Measurement.date >= start).filter(Measurement.date <= max_date).all()

    return jsonify(tmam[0])

@app.route("/api/v1.0/<start>/<end>")
def start_end(start, end):
    # Create our session (link) from Python to the DB
    session = Session(engine)

    tmam = session.query(func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)).\
        filter(Measurement.date >= start).filter(Measurement.date <= end).all()

    return jsonify(tmam[0])


if __name__ == '__main__':
    app.run(debug=True)
