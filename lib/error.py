# coding=utf-8

class Error(Exception):
    u"""通用错误
    """
    pass

class DownloadError(Error):
    u"""下载错误"""
    pass

class DataBaseError(Error):
    u"""数据库错误"""
    pass