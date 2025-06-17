FROM sergiofenoll/mu-python-template:latest

# Enable development mode by default to ensure the requests don't time out
# On production systems this service will probably look at a TON of files
# which would never be finished in under a minute, the default timeout
ENV MODE="development"