import requests
import json
import pyodbc
from django.shortcuts import render

# Connecting to SQL Server - Database
conn = pyodbc.connect('Driver={SQL Server};Server=.\; Database=eskompy;Trusted_Connection=yes;')  # conn-Connection
conn.timeout = 60  # The time to wait for a connection to open
conn.autocommit = True

# "Key" - Variable holding the token-key to access the APIs.
Key = "A03CC718-C3C546E0-BB8D4556-2CB16FE2"

def button (request):
    return render(request,'eskomInfo.html')

# GET-STATUS request
def status_output(request):

    # Connecting Eskom API and Fetching the Data
    response = requests.get("https://developer.sepush.co.za/business/2.0/status",headers={
                            "token": Key})
    data = response.json()  # converts the given json response to dictionary

    # Establishing Connection with API
    if response.status_code == 200:

        # Parsing the JSON Response and Assigning them to Variables

        # Capetown
        capetown_name = data["status"]["capetown"]["name"]
        capetown_presentStage = data["status"]["capetown"]["stage"]
        capetown_presentStage_updatedTime = data["status"]["capetown"]["stage_updated"]
        capetown_upcomingStages = data["status"]["capetown"]["next_stages"]

        # Eskom
        eskom_name = data["status"]["eskom"]["name"]
        eskom_presentStage = data["status"]["eskom"]["stage"]
        eskom_presentStage_updatedTime = data["status"]["eskom"]["stage_updated"]
        eskom_upcomingStages = data["status"]["eskom"]["next_stages"]

        # Creating a List of dictionaries holding required Output columns and corresponding records
        values_list = []
        if (len(capetown_upcomingStages) != 0):
            for next_stages in capetown_upcomingStages:
                capetown_dictionary = {
                    "name": capetown_name,
                    "present_stage": capetown_presentStage,
                    "present_stage_updatedTime": capetown_presentStage_updatedTime,
                    "next_stage": next_stages["stage"],
                    "next_stage_scheduledTime": next_stages["stage_start_timestamp"],
                }
                values_list.append(capetown_dictionary)
        else:
            capetown_dictionary = {
                "name": capetown_name,
                "present_stage": capetown_presentStage,
                "present_stage_updatedTime": capetown_presentStage_updatedTime,
                "next_stage": 'No Info by Eskom',
                "next_stage_scheduledTime": 'No Info by Eskom',
            }
            values_list.append(capetown_dictionary)

        if (len(eskom_upcomingStages) != 0):
            for next_stages in eskom_upcomingStages:
                eskom_dictionary = {
                    "name": eskom_name,
                    "present_stage": eskom_presentStage,
                    "present_stage_updatedTime": eskom_presentStage_updatedTime,
                    "next_stage": next_stages["stage"],
                    "next_stage_scheduledTime": next_stages["stage_start_timestamp"],
                }
                values_list.append(eskom_dictionary)
        else:
            eskom_dictionary = {
                "name": eskom_name,
                "present_stage": eskom_presentStage,
                "present_stage_updatedTime": eskom_presentStage_updatedTime,
                "next_stage": 'No Info by Eskom',
                "next_stage_scheduledTime": 'No Info by Eskom',
            }
            values_list.append(eskom_dictionary)

        getstatus_jsonstring = json.dumps(values_list) # converting the list of dictionaries into SQL readable JsonString

        # Calling the stored procedure and trap error if raised
        try:
            cursor = conn.cursor()  # An object that is used to make the connection for executing SQL queries
            cursor.execute('EXEC insertstatus @json=?', getstatus_jsonstring)
            print('Inserted Data')
            update='Data Loaded in SQL SERVER DB'

        except pyodbc.Error as err:
            print('Error !!! %s' % err)
            update=('Error !!! %s' % err)

        except:
            print('Something else failed')
            update=('Something else failed')

        conn.close()
        print('closed db connection')

    else:
        print("Request Failed , Couldn't Connect with API. ErrorCode: ", response.status_code)
        update=("Request Failed , Couldn't Connect with API. ErrorCode: ", response.status_code)

    return render(request,'eskomInfo.html',{'status':update})


#GET-AREA_SEARCH request - Using city name input as parameter.
def areasearch_output(request):

    # Connecting to SQL Server - Database
    conn = pyodbc.connect('Driver={SQL Server};Server=.\; Database=eskompy;Trusted_Connection=yes;')  # conn-Connection
    conn.timeout = 60  # The time to wait for a connection to open
    conn.autocommit = True

    #Fetching the input value from the frontend
    if request.method =="POST":
        area = request.POST.get('areaName')

        # Connecting Eskom API and Fetching the Data
        response = requests.get("https://developer.sepush.co.za/business/2.0/areas_search?text=" + area, headers={
                                "token": Key})
        data = response.json()  # converts the given json response to dictionary

        # Establishing Connection with API
        if response.status_code == 200:

            # Parsing the JSON Response and Assigning them to Variables
            area_details = data["areas"]

            # Creating a List of dictionaries holding required Ouput columns and corresponding records
            values_list = []
            for details in area_details:
                areasearch_dictionary = {
                    "AreaID": details["id"],
                    "Area_Name": details["name"],
                    "Region_of_Area": details["region"]
                }
                values_list.append(areasearch_dictionary)

            get_areasearch_jsonstring = json.dumps(
                values_list)  # converting the list of dictionaries into SQL readable JsonString

            # Calling the stored procedure and trap error if raised
            try:
                cursor = conn.cursor()  # An object that is used to make the connection for executing SQL queries
                cursor.execute('EXEC insertareasearch @json=?', get_areasearch_jsonstring)
                print('Inserted Data')
                update = area+' Data Loaded in SQL SERVER DB'

            except pyodbc.Error as err:
                print('Error !!! %s' % err)
                update =('Error !!! %s' % err)

            except:
                print('Something else failed')
                update = ('Something else failed')

            conn.close()
            print('closed db connection')

        else:
            print("Request Failed , Couldn't Connect with API ErrorCode: ", response.status_code)
            update=("Request Failed , Couldn't Connect with API ErrorCode: ", response.status_code)
        return render(request, 'eskomInfo.html', {'areaSearch': update})

#GET-AREA_SEARCH(GPS) request - Using GPS coordinates as input parameter.
def areasearchGPS_output(request):

    # Connecting to SQL Server - Database
    conn = pyodbc.connect('Driver={SQL Server};Server=.\; Database=eskompy;Trusted_Connection=yes;')  # conn-Connection
    conn.timeout = 60  # The time to wait for a connection to open
    conn.autocommit = True

    #Getting the Inputs from the frontend
    if request.method == "POST":
        lat = request.POST.get('latitude')
        lon = request.POST.get('longitude')

        # Connecting Eskom API and Fetching the Data
        response = requests.get("https://developer.sepush.co.za/business/2.0/areas_nearby?lat="+lat+"&lon="+lon,
                                headers={"token": Key})
        data = response.json()  # converts the given json response to dictionary

        # Establishing Connection with API
        if response.status_code == 200:

            # Parsing the JSON Response and Assigning them to Variables
            area_details = data["areas"]

            # Creating a List of dictionaries holding required output columns and corresponding records
            values_list = []
            for details in area_details:
                areasearchGPS_dictionary = {
                    "count": details["count"],
                    "areaID": details["id"],
                }
                values_list.append(areasearchGPS_dictionary)

            get_areasearchGPS_jsonstring = json.dumps(values_list)  # converting the list of dictionaries into SQL readable JsonString

            # Calling the stored procedure and trap error if raised
            try:
                cursor = conn.cursor()  # An object that is used to make the connection for executing SQL queries
                cursor.execute('EXEC insertareasearchGPS @json=?', get_areasearchGPS_jsonstring)
                print('Inserted Data')
                update = ' Close by AreaIDs data Loaded in SQL SERVER DB'

            except pyodbc.Error as err:
                print('Error !!! %s' % err)
                update=('Error !!! %s' % err)

            except:
                print('Something else failed')
                update=('Something else failed')

            conn.close()
            print('closed db connection')

        else:
            print("Request Failed , Couldn't Connect with API ErrorCode: ", response.status_code)
            update=("Request Failed , Couldn't Connect with API ErrorCode: ", response.status_code)

        return render(request, 'eskomInfo.html', {'areasearchGPS': update})

#GET-TOPICS_NEARBY request - Using GPS coordinates as input parameter.
def topicsNearby_output(request):

    # Connecting to SQL Server - Database
    conn = pyodbc.connect('Driver={SQL Server};Server=.\; Database=eskompy;Trusted_Connection=yes;')  # conn-Connection
    conn.timeout = 60  # The time to wait for a connection to open
    conn.autocommit = True

    #Getting the Inputs from the frontend
    if request.method == "POST":
        lat = request.POST.get('latitude')
        lon = request.POST.get('longitude')

        # Connecting Eskom API and Fetching the Data
        response = requests.get("https://developer.sepush.co.za/business/2.0/topics_nearby?lat="+lat+"&lon="+lon,
                                headers={"token": Key})
        data = response.json()  # converts the given json response to dictionary

        # Establishing Connection with API
        if response.status_code == 200:

            # Parsing the JSON Response and Assigning them to Variables
            topics_details = data["topics"]

            # Creating a List of dictionaries holding required output columns and corresponding records
            values_list = []
            for details in topics_details:
                topics_dictionary = {
                    "active": details["active"],
                    "body": details["body"],
                    "category": details["category"],
                    "distance": details["distance"],
                    "followers": details["followers"],
                    "timestamp": details["timestamp"]
                }
                values_list.append(topics_dictionary)

            get_topicsNearby_jsonstring = json.dumps(values_list)  # converting the list of dictionaries into SQL readable JsonString

            # Calling the stored procedure and trap error if raised
            try:
                cursor = conn.cursor()  # An object that is used to make the connection for executing SQL queries
                cursor.execute('EXEC inserttopicsNearby @json=?', get_topicsNearby_jsonstring)
                print('Inserted Data')
                update = ' Close by Area-Topics data Loaded in SQL SERVER DB'

            except pyodbc.Error as err:
                print('Error !!! %s' % err)
                update=('Error !!! %s' % err)

            except:
                print('Something else failed')
                update=('Something else failed')

            conn.close()
            print('closed db connection')

        else:
            print("Request Failed , Couldn't Connect with API ErrorCode: ", response.status_code)
            update=("Request Failed , Couldn't Connect with API ErrorCode: ", response.status_code)

        return render(request, 'eskomInfo.html', {'areaTopics': update})

#GET-AREA_INFORAMATION request - Fetching the current event details of the given area-id.
def areainfoEvent_output(request):

    # Connecting to SQL Server - Database
    conn = pyodbc.connect('Driver={SQL Server};Server=.\; Database=eskompy;Trusted_Connection=yes;')  # conn-Connection
    conn.timeout = 60  # The time to wait for a connection to open
    conn.autocommit = True

    # Getting the Inputs from the frontend
    if request.method == "POST":
        areaID=request.POST.get('areaID')

        # Connecting Eskom API and Fetching the Data
        response = requests.get("https://developer.sepush.co.za/business/2.0/area?id="+ areaID,
                                headers={"token": Key})
        data = response.json()  # converts the given json response to dictionary

        # Establishing Connection with API
        if response.status_code == 200:

            # Parsing the JSON Response and Assigning them to Variables
            event_details = data["events"]
            areaName = data["info"]["name"]
            region = data["info"]["region"]

            # Creating a List of dictionaries holding required output columns and corresponding records
            values_list = []
            for details in event_details:
                events_dictionary = {
                    "areaName": areaName,
                    "region": region,
                    "endDatetime": details["end"],
                    "startDatetime": details["start"],
                    "stagetoExecute": details["note"]
                }
                values_list.append(events_dictionary)

            AreainfoEvent_jsonstring = json.dumps(
                values_list)  # converting the list of dictionaries into SQL readable JsonString

            # Calling the stored procedure and trap error if raised
            try:
                cursor = conn.cursor()  # An object that is used to make the connection for executing SQL queries
                cursor.execute('EXEC insertAreaInfoEvent @json=?', AreainfoEvent_jsonstring)
                print('Inserted Data')
                update = 'Event Data for the given ID is Loaded in SQL SERVER DB'

            except pyodbc.Error as err:
                print('Error !!! %s' % err)
                update = ('Error !!! %s' % err)

            except:
                print('Something else failed')
                update= ('Something else failed')

            conn.close()
            print('closed db connection')

        else:
            print("Request Failed , Couldn't Connect with API ErrorCode: ", response.status_code)
            update=("Request Failed , Couldn't Connect with API ErrorCode: ", response.status_code)
        return render(request, 'eskomInfo.html', {'areainfoEvent': update})

#GET-AREA_INFORAMATION request - Fetching the upcoming schedule details for the given area-id.
def areainfoSchedule_output(request):

    # Connecting to SQL Server - Database
    conn = pyodbc.connect('Driver={SQL Server};Server=.\; Database=eskompy;Trusted_Connection=yes;')  # conn-Connection
    conn.timeout = 60  # The time to wait for a connection to open
    conn.autocommit = True

    # Getting the Inputs from the frontend
    if request.method == "POST":
        areaID = request.POST.get('areaID')

        # Connecting Eskom API and Fetching the Data
        response = requests.get("https://developer.sepush.co.za/business/2.0/area?id=" + areaID,
                                headers={"token": Key})
        data = response.json()  # converts the given json response to dictionary

        # Establishing Connection with API
        if response.status_code == 200:

            # Parsing the JSON Response and Assigning them to Variables
            areaName = data["info"]["name"]
            region = data["info"]["region"]
            schedules = data["schedule"]["days"]

            # Creating a List of dictionaries holding required output columns and corresponding records
            values_list = []
            for values in schedules:
                stage=values["stages"]
                date=values["date"]
                day=values["name"]
                for items in range(0,len(stage)):
                    for elements in range(0,len(stage[items])):
                        schedules_dictionary = {
                            "areaName": areaName,
                            "region": region,
                            "date": date,
                            "day": day,
                            "stageTimings": stage[items][elements]
                            }
                        values_list.append(schedules_dictionary)

            AreainfoSchedule_jsonstring = json.dumps(values_list)  # converting the list of dictionaries into SQL readable JsonString

            # Calling the stored procedure and trap error if raised
            try:
                cursor = conn.cursor()  # An object that is used to make the connection for executing SQL queries
                cursor.execute('EXEC insertAreaInfoSchedule @json=?', AreainfoSchedule_jsonstring)
                print('Inserted Data')
                update = 'Schedules loaded in SQL SERVER DB'

            except pyodbc.Error as err:
                print('Error !!! %s' % err)
                update= ('Error !!! %s' % err)

            except:
                print('Something else failed')
                update= ('Something else failed')

            conn.close()
            print('closed db connection')

        else:
            print("Request Failed , Couldn't Connect with API ErrorCode: ", response.status_code)
            update=("Request Failed , Couldn't Connect with API ErrorCode: ", response.status_code)

        return render(request, 'eskomInfo.html', {'areainfoSchedule': update})

def keyallowance_output(request):
    # Connecting to SQL Server - Database
    conn = pyodbc.connect('Driver={SQL Server};Server=.\; Database=eskompy;Trusted_Connection=yes;')  # conn-Connection
    conn.timeout = 60  # The time to wait for a connection to open
    conn.autocommit = True

    # Connecting Eskom API and Fetching the Data
    response = requests.get("https://developer.sepush.co.za/business/2.0/api_allowance", headers={
                            "token": Key})
    data = response.json()  # converts the given json response to dictionary

    # Establishing Connection with API
    if response.status_code == 200:
        # Parsing the JSON Response and Assigning them to Variables

        # Key Allowance
        count = data["allowance"]["count"]
        limit = data["allowance"]["limit"]
        type = data["allowance"]["type"]

        # Creating a List of dictionaries holding required output columns and corresponding records
        values_list = []
        keyAllowance_dictionary={
            "count": count,
            "limit": limit,
            "type": type
        }
        values_list.append(keyAllowance_dictionary)

        keyAllowance_jsonstring = json.dumps(values_list)

        # Calling the stored procedure and trap error if raised
        try:
            cursor = conn.cursor()  # An object that is used to make the connection for executing SQL queries
            cursor.execute('EXEC insertKeyAllowance @json=?', keyAllowance_jsonstring)
            print('Inserted Data')
            update ='Key Allowance Data Loaded to SQL Server, The key used has consumed %d/%d allowances ' %(count,limit)

        except pyodbc.Error as err:
            print('Error !!! %s' % err)
            update = ('Error !!! %s' % err)

        except:
            print('Something else failed')
            update = ('Something else failed')

        conn.close()
        print('closed db connection')

    else:
        print("Request Failed , Couldn't Connect with API ErrorCode: ", response.status_code)
        update = ("Request Failed , Couldn't Connect with API ErrorCode: ", response.status_code)

    return render(request, 'eskomInfo.html', {'keyAllowance': update})















