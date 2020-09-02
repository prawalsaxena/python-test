from mysql import connector
# if any new file needs to be copied, add the file name here
file_names = ['broadcast.sql', 'cross_channel.sql', 'digital.sql', 'offline.sql', 'paid_search.sql', 'social.sql']
# connect to db
cnx = connector.connect(
    user=,
    password=,
    database=,
    host=
)
error_in_process = False
for each_file_name in file_names:
    print("reading file - " + each_file_name)
    cursor = cnx.cursor()
    file_name_without_extension = each_file_name.split('.sql')[0]
    try:
        file = open(each_file_name, 'r')
        file_content = file.read()
        file.close()
    except Exception as e:
        error_in_process = True
        print(e)
        print("ERROR - Exiting process")
        break
    file_content = file_content.strip()
    # the file name without the extension should be in the description column of the sql_script table
    # Python 2.7
    # query = 'update sql_script set `script` = ' + '"' + file_content + '"' + " where `key` = 'upd_master_view' and description = " + '"' + file_name_without_extension + '";'
    # Python 3.7
    query = f'update sql_script set `script` = "{file_content}" where `key` = "upd_master_view" and description = "{file_name_without_extension}";'
    cursor.execute(query)
    cursor.close()
    cnx.commit()
    print("file data copied")
cnx.close()
if error_in_process:
    print("process completed with error(s)")
else:
    print("process completed successfully")
