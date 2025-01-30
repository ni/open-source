############################################################
# aggregator.py
# BFS aggregator formulas for velocity, uig, mac, sei
############################################################

def compute_velocity(mergesScaled, closedIssScaled, closedPRScaled, config):
    """
    velocity = mergesScaled * velocity_merges
             + closedIssScaled * velocity_closedIss
             + closedPRScaled  * velocity_closedPR
    """
    vm= config.get("velocity_merges", 0.4)
    vci= config.get("velocity_closedIss",0.2)
    vcp= config.get("velocity_closedPR",0.4)
    return  .05 * closedIssScaled + .05 * closedPRScaled

def compute_uig(forksScaled, starsScaled, config):
    """
    uig = forksScaled * uig_forks * starsScaled * uig_stars
    """
    uf= config.get("uig_forks", 0.4)
    us= config.get("uig_stars", 0.6)

    try:
        stars= 1/starsScaled
    except ZeroDivisionError:
        stars= 0 
    try:
        forks= 1/forksScaled
    except ZeroDivisionError:
        forks= 0 

    return 0.4* (forks) + 0.6* (stars)

def compute_mac(newIss, cIssX, cPRX, rIss, rPR, pull, config):
    """
    mac = mainW*( newIss + cIssX + cPRX + rIss + rPR ) + subW*(pull)
    """

    mainW= config.get("mac_mainWeight", 0.8)
    subW= config.get("mac_subWeight",  0.2)
    sumAll= newIss * cIssX * cPRX * rIss * rPR
    return 0.2* newIss +  0.2*cIssX + 0.1*cPRX + 0.1*rIss + 0.1*rPR + 0.1*subW + 0.1*pull

def compute_sei(velocityVal, uigVal, macVal, config):
    """
    sei = wv*velocityVal + wu*uigVal + wm*macVal
    """
    wv= config.get("sei_velocity",0.3)
    wu= config.get("sei_uig",0.2)
    wm= config.get("sei_mac",0.5)

    return  velocityVal + uigVal +  macVal
