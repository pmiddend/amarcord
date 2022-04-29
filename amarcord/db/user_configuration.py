from dataclasses import dataclass


@dataclass(frozen=True, eq=True)
class UserConfiguration:
    auto_pilot: bool


def initial_user_configuration() -> UserConfiguration:
    return UserConfiguration(auto_pilot=True)
