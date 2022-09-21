from dataclasses import dataclass


@dataclass(frozen=True, eq=True)
class UserConfiguration:
    auto_pilot: bool
    use_online_crystfel: bool


def initial_user_configuration() -> UserConfiguration:
    return UserConfiguration(auto_pilot=True, use_online_crystfel=False)
