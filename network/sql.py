'''
Run remote SQL commands off a replica DB.

This uses an obsolete settings configuration.
'''

import os, os.path
import paramiko

from edxconfig import setting

queries = {
    # Number of certificates student earned
    'student_certs' : 'select username, certs from (select user_id as uid, count(distinct certificates_generatedcertificate.id) as certs from certificates_generatedcertificate where certificates_generatedcertificate.status = "downloadable" group by user_id) as a, auth_user where auth_user.id = uid order by certs;',
    # Basic demographics
    'basic_info': 'select auth_user.username, auth_user.date_joined, auth_user.email, auth_user.id, auth_user.is_active, auth_userprofile.year_of_birth, auth_userprofile.gender, auth_userprofile.level_of_education from auth_user, auth_userprofile where auth_user.id = auth_userprofile.user_id'
    }

test = 'select username, id from auth_user limit 10'

_ssh = None
def get_ssh():
    global _ssh
    if not _ssh:
        _ssh = paramiko.SSHClient()
        _ssh.load_system_host_keys()
        _ssh.connect(setting('replica'))
    return _ssh

def sqlquery(query, file):
    '''Run the query over the read-replica DB. Store the results in a file

    Known issues: 
    * The query is passed over a Unix commandline in single quotes. We
      should clean this up, but for now, don't use single quotes.
    * This code is *intentionally* *not* *secure*. It presumes you
    have ssh access to the replica. It should *not* be co-opted for
    secure applications.

    '''
    ssh = get_ssh()
    command = "echo '{query}' | {replica}".format(query=query, replica=setting('replica-sql'))
    ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command(command)
    with open(file, "w") as f:
        for x in ssh_stdout:
            f.write(x)

def populate_tables():
    for query in queries:
        sqlquery(queries[query], os.path.join(setting("userinfo"), query+".csv"))

if __name__ == "__main__":
    populate_tables()
