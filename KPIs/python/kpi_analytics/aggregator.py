############################################
# aggregator.py
############################################

def compute_velocity(mergesScaled, closedIssScaled, closedPRScaled, config):
    """
    velocity = v_mergesFactor * mergesScaled
             + v_closedIssFactor * closedIssScaled
             + v_closedPRFactor  * closedPRScaled
    """
    v_m= config.get('velocity_merges', 0.4)
    v_ci= config.get('velocity_closedIss', 0.2)
    v_cp= config.get('velocity_closedPR', 0.4)
    val= v_m* mergesScaled + v_ci* closedIssScaled + v_cp* closedPRScaled
    return val

def compute_uig(forksScaled, starsScaled, config):
    """
    uig = u_forks * forksScaled + u_stars * starsScaled
    """
    u_f= config.get('uig_forks', 0.4)
    u_s= config.get('uig_stars', 0.6)
    return (u_f* forksScaled + u_s* starsScaled)

def compute_mac(newIssScaled, comIssScaled, comPRScaled, reactIssScaled, reactPRScaled, pullScaled, config):
    """
    mac = mainW*(newIss+comIss+comPR+reactIss+reactPR) + subW*(pulls)
    """
    mainW= config.get('mac_mainWeight', 0.8)
    subW= config.get('mac_subWeight', 0.2)
    sumAll= newIssScaled + comIssScaled + comPRScaled + reactIssScaled + reactPRScaled
    val= mainW* sumAll + subW* pullScaled
    return val

def compute_sei(velocityVal, uigVal, macVal, config):
    """
    sei = wv*velocityVal + wu*uigVal + wm*macVal
    """
    wv= config.get('sei_velocity', 0.3)
    wu= config.get('sei_uig', 0.2)
    wm= config.get('sei_mac', 0.5)
    return (wv* velocityVal + wu* uigVal + wm* macVal)
