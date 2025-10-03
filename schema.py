from pydantic import BaseModel, Field, model_validator
from typing import Optional
from datetime import datetime

class IgnoreExtraModel(BaseModel):
    def __str__(self):
        return self.model_dump_json(indent=2)

    model_config = {
        "extra": "ignore"
    }

    @model_validator(mode="before")
    def convert_created_at(self, values):
        if "created_at" in values and values["created_at"]:
            values["created_at"] = to_iso_format(values["created_at"])
        return values


class User(IgnoreExtraModel):
    id: int
    name: str | None = Field(default='')
    screen_name: str | None = Field(default='')
    location: str | None = Field(default='')
    url: str | None = Field(default='')
    description: str | None = Field(default='')
    protected: bool | None = Field(default=None)
    verified: bool | None = Field(default=None)
    followers_count: int | None = Field(default=0)
    friends_count: int | None = Field(default=0)
    statuses_count: int | None = Field(default=0)
    created_at: str | None = Field(default=None) # UTC datetime


class Place(IgnoreExtraModel):
    id: str
    place_type: str | None = Field(default='')
    full_name: str | None = Field(default='')
    country_code: str | None = Field(default='')
    country: str | None = Field(default='')


class Hashtag(BaseModel):
    text: str | None = Field(default='')


class UnwoundUrl(IgnoreExtraModel):
    url: str | None = Field(default='')
    status: int | None = Field(default=None)
    title: str | None = Field(default='')
    description: str | None = Field(default='')


class Url(IgnoreExtraModel):
    url: str | None = Field(default='')
    expanded_url: str | None = Field(default='')
    display_url: str | None = Field(default='')
    unwound_url: UnwoundUrl | None = Field(default=None)


class Media(IgnoreExtraModel):
    id: int
    display_url: str | None = Field(default='')
    expanded_url: str | None = Field(default='')
    media_url: str | None = Field(default='')
    media_url_https: str | None = Field(default='')
    type: str | None = Field(default='')


class UserMention(IgnoreExtraModel):
    id: int # User ID
    screen_name: str | None = Field(default='')
    name: str | None = Field(default='')

class Entities(IgnoreExtraModel):
    hashtags: list[Hashtag] | None = Field(default_factory=list)
    user_mentions: list[UserMention] | None = Field(default_factory=list)
    urls: list[Url] | None = Field(default_factory=list)
    media: list[Media] | None = Field(default_factory=list)


class Tweet(IgnoreExtraModel):
    id: int
    text: str | None = Field(default='')
    source: str | None = Field(default='')
    in_reply_to_status_id: int | None = Field(default=None)
    quoted_status_id: int | None = Field(default=None)
    retweet_count: int | None = Field(default=0)
    favorite_count: int | None = Field(default=0)
    created_at: str | None = Field(default='') # UTC datetime
    lang: str | None = Field(default='')
    possibly_sensitive: bool | None = Field(default=None)
    display_text_range: tuple[int, int] | None = Field(default=None)
    retweeted_status: Optional['Tweet'] = Field(default=None)
    quoted_status: Optional['Tweet'] = Field(default=None)
    user: User | None = Field(default=None)
    place: Place | None = Field(default=None)
    entities: Entities | None = Field(default=None)


def to_iso_format(date_str: str) -> str:
    # If already in ISO format, return as is
    try:
        # Try parsing as ISO format
        datetime.fromisoformat(date_str)
        return date_str
    except ValueError:
        pass
    # Otherwise, parse as Twitter format
    try:
        dt = datetime.strptime(date_str, '%a %b %d %H:%M:%S %z %Y')
        return dt.isoformat()
    except ValueError: # If can't parse, return original string, cause it's better than nothing
        return date_str