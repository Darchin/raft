from collections import deque


class MemoryBoard(object):

    def __init__(self):
        self._board = deque()

    def post_message(self, recipient, message):
        self._board.append((recipient, message))

    def get_message(self):
        if(len(self._board) > 0):
            return self._board.popleft()
        else:
            return (None, None)
