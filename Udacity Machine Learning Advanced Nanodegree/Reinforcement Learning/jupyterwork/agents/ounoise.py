import numpy as np
import random

class OUNoise():
    # Ornstein-Uhlenbeck process
    def __init__(self, size, mu, theta, sigma):
        # initialize parameters and noise process
        self.mu = mu*np.ones(size)
        self.theta = theta
        self.sigma = sigma
        self.reset()
        
    def reset(self):
        # reset internal state to mean
        self.state = self.mu
        
    def sample(self):
        # update internal state and return it as a noise sample
        x = self.state
        dx = self.theta*(self.mu - x) + self.sigma*np.random.randn(len(x))
        self.state = x + dx
        return self.state