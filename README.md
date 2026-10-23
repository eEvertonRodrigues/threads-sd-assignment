./run_clients.sh <número de clientes> <número de workers> <quantidade de números a ser computados>
ghcpi -threaded -rtsopts client.hs
ghci -threaded -rtsopts server.hs                       