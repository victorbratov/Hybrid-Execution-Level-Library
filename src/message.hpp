#pragma once
#include "./payload.hpp"

struct Message {
	Payload payload;
	bool    eos = false;
};
