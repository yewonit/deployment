echo "tunneling..."
ssh -i ./isaac-keypair.pem -N -L 3306:localhost:3306 ec2-user@43.202.167.45
