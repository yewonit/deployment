echo "tunneling..."
ssh -i ./isaac-keypair.pem -N -L 3306:coramdeo-database.cpkaaeym4mdn.ap-northeast-2.rds.amazonaws.com:3306 ec2-user@43.202.167.45
