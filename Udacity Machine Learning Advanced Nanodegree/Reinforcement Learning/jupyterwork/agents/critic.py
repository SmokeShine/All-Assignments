import numpy as np
import task as Task
import random
from collections import deque, namedtuple
from keras import layers,models,optimizers
from keras import backend as K
import pdb

class Critic():
    def __init__(self,state_size,action_size):
        self.state_size = state_size
        self.action_size = action_size
        self.build_model()        
    def build_model(self):
        #         pdb.set_trace()
        states = layers.Input(shape=(self.state_size,),name='states')
        actions = layers.Input(shape=(self.action_size,),name='actions')
        states_net = layers.Dense(units=32, activation='relu')(states)
        states_net = layers.Dense(units=64, activation='relu')(states_net)
        actions_net = layers.Dense(units=32, activation='relu')(actions)
        actions_net = layers.Dense(units=64, activation='relu')(actions_net)
        net = layers.Add()([states_net,actions_net])
        net = layers.Activation('relu')(net)
        Q_values = layers.Dense(units=1, name='Q_values')(net)
        self.model = models.Model(inputs=[states,actions], outputs=Q_values)
        optimizer = optimizers.Adam()
        self.model.compile(optimizer=optimizer, loss='mse')
        action_gradients = K.gradients(Q_values,actions)
        self.get_action_gradients = K.function(inputs=[*self.model.input,K.learning_phase()],outputs=action_gradients)