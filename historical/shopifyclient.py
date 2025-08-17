import shopify




def shopify_client(merchant, token):
    session = shopify.Session(merchant, '2025-01', token)
    shopify.ShopifyResource.activate_session(session)
    return shopify.GraphQL()



    
    