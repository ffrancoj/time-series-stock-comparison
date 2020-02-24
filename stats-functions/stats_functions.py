import math


def stdf(vec):
    
    """ Custom standard deviation of a vector [v1,...,vn]: namely sqrt(v1^2+...+vn^2)."""
    
    prod = [x*x for x in vec]
    s = sum(prod)/len(vec)
    a = (sum(vec)/len(vec))**2
    return math.sqrt(s-a)


def correl(vec1,vec2):
    
    """ Custom correlation of two vectors vec1, vec2. Returns 0 when they don't have the same length."""
    
    l1=len(vec1)
    if len(vec1)==len(vec2):
        prod = list(map(lambda x,y: x*y,vec1,vec2))
        a1 = sum(vec1)/l1
        a2 = sum(vec2)/l1
        a3 = sum(prod)/l1
        return (a3-a1*a2)/(stdf(vec1)*stdf(vec2))
    else:
        return 0

    
    
    
    
    

    
