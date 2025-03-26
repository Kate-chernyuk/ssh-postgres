import sys
import paramiko
import subprocess
import ansible
import asyncio
import asyncpg
from sshtunnel import SSHTunnelForwarder

def get_private_key():
    key_file_path = '/home/kali/Desktop/private_key'  
    try:
        key = paramiko.RSAKey(filename=key_file_path)
        return key
    except paramiko.SSHException as e:
        print(f"Ошибка загрузки ключа: {e}")
        sys.exit(1)

def execute_command(client, command):
    stdin, stdout, stderr = client.exec_command(command)
    exit_status = stdout.channel.recv_exit_status() 
    if exit_status != 0:
        print(f"Ошибка выполнения команды '{command}': {stderr.read().decode()}")
    return stdout.read().decode()

def get_server_load(server, key):
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(server, username='root', pkey=key)

    load = execute_command(client, "uptime | awk '{print $10}'")
    client.close()
    return float(load.strip().replace(',', '.'))

def install_postgresql(target, key):
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(target, username='root', pkey=key)

    try:
        execute_command(client, "sudo apt-get install postgresql postgresql-contrib")
        execute_command(client, "sudo -u postgres psql -c \"ALTER USER postgres WITH PASSWORD '123456';\"")
        execute_command(client, "test -d /var/lib/postgresql/16/main || sudo -u postgres /usr/lib/postgresql/16/bin/initdb -D /var/lib/postgresql/16/main")
        execute_command(client, "systemctl start postgresql")
        execute_command(client, "systemctl enable postgresql")
        print("PostgreSQL успешно установлен")
        #print(execute_command(client, "systemctl status postgresql"))
    except Exception as e:
        print(f"Не удалось установить PostgreSQL: {e}")

    client.close()

def configure_postgresql(server, key):
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(server, username='root', pkey=key)

    try:
        execute_command(client, "echo \"host all postgres 0.0.0.0/0 md5\" | sudo tee -a /etc/postgresql/16/main/pg_hba.conf")
        execute_command(client, "sudo sed -i 's/#listen_addresses = 'localhost'/listen_addresses = '*'/' /etc/postgresql/16/main/postgresql.conf")
        execute_command(client, "systemctl restart postgresql")
        print("PostgreSQL успешно настроен")
    except Exception as e:
        print(f"Не удалось настроить PostgreSQL: {e}")

    client.close()

def enable_tcp_forwarding(server, key):
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(server, username='root', pkey=key)

    try:
        execute_command(client, "sudo sed -i 's/#AllowTcpForwarding yes/AllowTcpForwarding yes/' /etc/ssh/sshd_config")
        execute_command(client, "sudo sed -i 's/#GatewayPorts no/GatewayPorts yes/' /etc/ssh/sshd_config")
        execute_command(client, "sudo systemctl restart ssh || sudo systemctl restart sshd")
        print("Есть TCP forwarding")
    except Exception as e:
        print(f"Ошибка при включении TCP forwarding: {e}")

    client.close()

async def check_postgresql(server, key):
    db_name = 'postgres'
    db_user = 'postgres'
    db_password = '123456' 
    db_host = '127.0.0.1' 
    db_port = 5432
    local_port = 5433

    try:
        with SSHTunnelForwarder(
            (server, 22),
            ssh_username='root',
            ssh_pkey=key,
            remote_bind_address=(db_host, db_port),
            local_bind_address=('127.0.0.1', local_port)
            ) as tunnel:
            conn = await asyncpg.connect(
                user=db_user,
                password=db_password,
                database=db_name,
                host='127.0.0.1',
                port=tunnel.local_bind_port
                )

            result = await conn.fetch("SELECT 1;")
            print(f"Результат проверочного запроса: {result[0][0]}")

            await conn.close()

            return result[0][0] == 1

    except Exception as e:
        print(f"Не удалось проверить, т.к. {e}")
        return False
    
    finally:
        if conn in locals():
            await conn.close()

if __name__ == "__main__":
    private_key = get_private_key()
    servers = sys.argv[1].split(',')
    
    load_availability = {server: get_server_load(server, private_key) for server in servers}
    target_server = min(load_availability, key=load_availability.get)

    print(f"Выбранный сервер: {target_server}")

    try:
        print("Произведём установку...")
        install_postgresql(target_server, private_key)
        print("Успешно")
    except:
        print(f"Не удалось.")
        
    configure_postgresql(target_server, private_key)
    enable_tcp_forwarding(target_server, private_key)

    if asyncio.run(check_postgresql(target_server, private_key)):
        print("PostgreSQL успешно установлен и работает.")
    else:
        print("Ошибка при проверке работы PostgreSQL.")
