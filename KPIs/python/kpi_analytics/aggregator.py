############################################
# aggregator.py
############################################
def compute_velocity(mergesScaled, closedIssScaled, closedPRScaled, config):
    """
    velocity = v_merges*mergesScaled + v_closedIss*closedIssScaled + v_closedPR*closedPRScaled
    Default weights might be 0.4 merges, 0.2 closedIss, 0.4 closedPR, or any config you prefer.
    """
    v_m= config.get('velocity_merges', 0.4)
    v_ci= config.get('velocity_closedIss', 0.2)
    v_cp= config.get('velocity_closedPR', 0.4)
    val= v_m* mergesScaled + v_ci* closedIssScaled + v_cp* closedPRScaled
    return val

def compute_uig(forksScaled, starsScaled, config):
    """
    uig = uig_forks * forksScaled + uig_stars * starsScaled
    Default might be 0.4 forks, 0.6 stars.
    """
    f_w= config.get('uig_forks', 0.4)
    s_w= config.get('uig_stars', 0.6)
    val= f_w* forksScaled + s_w* starsScaled
    return val

def compute_mac(newIssScaled, commentsIssScaled, commentsPRScaled, reactIssScaled, reactPRScaled, pullScaled, config):
    """
    mac = mainWeight*(sumOf splitted comment+reaction+newIss) + subWeight*(pullScaled)
    e.g. mainWeight=0.8, subWeight=0.2
    """
    mainW= config.get('mac_mainWeight', 0.8)
    subW= config.get('mac_subWeight', 0.2)
    sumAll= newIssScaled + commentsIssScaled + commentsPRScaled + reactIssScaled + reactPRScaled
    val= mainW* sumAll + subW* pullScaled
    return val

def compute_sei(velocityVal, uigVal, macVal, config):
    """
    sei = wv*velocity + wu*uig + wm*mac
    e.g. wv=0.3, wu=0.2, wm=0.5
    """
    wv= config.get('sei_velocity', 0.3)
    wu= config.get('sei_uig', 0.2)
    wm= config.get('sei_mac', 0.5)
    val= wv* velocityVal + wu* uigVal + wm* macVal
    return val
