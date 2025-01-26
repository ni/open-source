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
    return vm* mergesScaled + vci* closedIssScaled + vcp* closedPRScaled

def compute_uig(forksScaled, starsScaled, config):
    """
    uig = forksScaled * uig_forks + starsScaled * uig_stars
    """
    uf= config.get("uig_forks", 0.4)
    us= config.get("uig_stars", 0.6)
    return uf* forksScaled + us* starsScaled

def compute_mac(newIss, cIssX, cPRX, rIss, rPR, pull, config):
    """
    mac = mainW*( newIss + cIssX + cPRX + rIss + rPR ) + subW*(pull)
    """
    mainW= config.get("mac_mainWeight", 0.8)
    subW= config.get("mac_subWeight",  0.2)
    sumAll= newIss + cIssX + cPRX + rIss + rPR
    return mainW* sumAll + subW* pull

def compute_sei(velocityVal, uigVal, macVal, config):
    """
    sei = wv*velocityVal + wu*uigVal + wm*macVal
    """
    wv= config.get("sei_velocity",0.3)
    wu= config.get("sei_uig",0.2)
    wm= config.get("sei_mac",0.5)
    return wv* velocityVal + wu* uigVal + wm* macVal
