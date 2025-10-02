from pydantic import BaseModel, Field

class IgnoreExtraModel(BaseModel):
    class Config:
        extra = 'ignore'


class User(IgnoreExtraModel):
    id: int
    name: str = Field(default='')
    screen_name: str = Field(default='')
    location: str = Field(default='')
    url: str = Field(default='')
    description: str = Field(default='')
    protected: bool | None = Field(default=None)
    verified: bool | None = Field(default=None)
    followers_count: int = Field(default=0)
    friends_count: int = Field(default=0)
    listed_count: int = Field(default=0)
    favourites_count: int = Field(default=0)
    statuses_count: int = Field(default=0)
    created_at: str | None = Field(default=None) # UTC datetime


class Place(IgnoreExtraModel):
    id: str = Field(default='')
    place_type: str = Field(default='')
    full_name: str = Field(default='')
    country_code: str = Field(default='')
    country: str = Field(default='')


class Hashtag(BaseModel):
    text: str = Field(default='')


class UnwoundUrl(IgnoreExtraModel):
    url: str = Field(default='')
    status: int | None = Field(default=None)
    title: str = Field(default='')
    description: str = Field(default='')


class Url(IgnoreExtraModel):
    url: str = Field(default='')
    expanded_url: str = Field(default='')
    display_url: str = Field(default='')
    unwound_url: UnwoundUrl | None = Field(default=None)


class Media(IgnoreExtraModel):
    id: int
    display_url: str = Field(default='')
    expanded_url: str = Field(default='')
    media_url: str = Field(default='')
    media_url_https: str = Field(default='')
    type: str = Field(default='')


class UserMention(IgnoreExtraModel):
    id: int # User ID
    screen_name: str = Field(default='')
    name: str = Field(default='')

class Entities(IgnoreExtraModel):
    hashtags: list[Hashtag] = Field(default_factory=list)
    user_mentions: list[UserMention] = Field(default_factory=list)
    urls: list[Url] = Field(default_factory=list)
    media: list[Media] | None = Field(default=None)


class Tweet(IgnoreExtraModel):
    id: int
    text: str = Field(default='')
    source: str = Field(default='')
    in_reply_to_status_id: int | None = Field(default=None)
    quoted_status_id: int | None = Field(default=None)
    retweet_count: int = Field(default=0)
    favorite_count: int = Field(default=0)
    created_at: str = Field(default='') # UTC datetime
    lang: str = Field(default='')
    possibly_sensitive: bool | None = Field(default=None)
    display_text_range: tuple[int, int] | None = Field(default=None)
    retweeted_status: BaseModel = Field(default=None) # References another Tweet object
    user: User = Field(default=None)
    place: Place = Field(default=None)
    entities: Entities = Field(default=None)


