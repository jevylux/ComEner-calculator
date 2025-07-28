import sqlite3 as lite

db = '/Users/marcdurbach/Development/python/ComEner-data-management/database/commEnergy.db'

def getMemberIDAndPodID(podNumber):
    # find podId
    con = lite.connect(db)
    cur = con.cursor()
    cur.execute("SELECT memberID, podsID  FROM pods WHERE podNumber = ?", (podNumber,))
    result = cur.fetchone()
    con.close()
    if result:
        memberID, podID = result
        return memberID, podID
    else:
        print("No pod found with the given pod number")
        return None, None

    if result:
        return result
    else:
        print("No record found")

def createOrUpdateAccounting(memberID, podID, sgID, year=None, month=None, amount=0):
    # create or update accounting data for the given memberID and podID

    conn = lite.connect(db)
    cur = conn.cursor()
    
    # Check if record exists
    cur.execute("SELECT * FROM accounting WHERE accMember = ? AND accPod = ? AND accYear = ? AND accMonth = ? AND accSGId = ?", (memberID, podID, year, month, sgID))
    record = cur.fetchone()
    
    if record:
        # Update existing record
        print("update with ", memberID, podID, sgID, year, month, amount,)
        cur.execute("UPDATE accounting SET accAmount = ? ,  accBillingDate = ? WHERE accMember = ? AND accPod = ? AND accYear = ? AND accMonth = ? AND accSGId = ?", (amount,  None , memberID, podID, year, month, sgID))
        print(f"Updated accounting for member {memberID} and pod {podID}")
    else:
        # Insert new record
        print("create with ", memberID, podID, sgID, year, month, amount)
        cur.execute("INSERT  INTO accounting (accMember, accPod, accYear, accMonth, accAmount, accSGId) VALUES (?, ?, ?, ?, ?, ?)", (memberID, podID, year, month, amount, sgID))
        print(f"Inserted new accounting for member {memberID} and pod {podID}")
    
    conn.commit()
    conn.close()