#include "buffer.h"
#include <unistd.h>

Buffer::Buffer(size_t capacity) {
    head = 0;
    tail = 0;
    _size = 0;
    this->capacity = capacity;
    data = new uint8_t[capacity];
}

Buffer::~Buffer() {
    delete[] data;
}

size_t Buffer::size() const {
    return _size;
}

void Buffer::append(const uint8_t *data, size_t len) {
    if (len + _size > capacity) {
        // if len + _size < 1m, resize to 2 * (len + _size)
        // if len + _size >= 1m, resize to  (len + _size) + 1m
        size_t new_capacity = (len + _size < 1024 * 1024) ?  (len + _size) * 2: len + _size + 1024 * 1024;
        resize(new_capacity);
    }
    if (tail + len > capacity) {
        size_t right = capacity - tail;
        size_t left = len - right;
        memcpy(this->data + tail, data, right);
        memcpy(this->data, data + right, left);
        tail = left;
    } else {
        memcpy(this->data + tail, data, len);
        tail += len;
    }
    _size += len;
}

void Buffer::append_u8(uint8_t data) {
    append(&data, 1);
}

void Buffer::append_u32(uint32_t data) {
    append((uint8_t *)&data, 4);
}

void Buffer::append_i64(int64_t data) {
    append((uint8_t *)&data, 8);
}

void Buffer::append_dbl(double data) {
    append((uint8_t *)&data, 8);
}

void Buffer::consume(size_t len) {
    head = (head + len) % capacity;
    _size -= len;
}

void Buffer::resize(size_t new_capacity) {
    uint8_t *new_data = new uint8_t[new_capacity];
    if (head < tail) {
        memcpy(new_data, data + head, _size);
    } else {
        size_t right = capacity - head;
        memcpy(new_data, data + head, right);
        memcpy(new_data + right, data, tail);
    }
    head = 0;
    tail = _size;
    capacity = new_capacity;
    delete[] data;
    data = new_data;
}

void Buffer::peek(uint8_t* dst, size_t pos, size_t len) const {
    if (pos >= _size) {
        return;
    }

    size_t real_pos = (head + pos) % capacity;

    if (real_pos + len <= capacity) {
        memcpy(dst, data + real_pos, len);
    } else {
        size_t right = capacity - real_pos;
        size_t left = len - right;
        memcpy(dst, data + real_pos, right);
        memcpy(dst + right, data, left);
    }
}

uint32_t Buffer::peek_u32(size_t pos) const {
    uint32_t res;
    peek((uint8_t *)&res, pos, 4);
    return res;
}

void Buffer::get_continuous_data(size_t pos, uint8_t** data_ptr, size_t* size) const {
    if (pos >= _size) {
        *data_ptr = nullptr;
        *size = 0;
        return;
    }

    size_t real_pos = (head + pos) % capacity;

    if (real_pos + _size <= capacity) {
        *data_ptr = data + real_pos;
        *size = tail - real_pos;
    } else {
        size_t right = capacity - real_pos;
        *data_ptr = data + real_pos;
        *size = right;
    }
}

void Buffer::copy_data(uint8_t *dst, size_t len) const {
    peek(dst, 0, len);
}

uint8_t& Buffer::operator[](size_t pos) {
    return data[(head + pos) % capacity];
}

const uint8_t& Buffer::operator[](size_t pos) const {
    return data[(head + pos) % capacity];
}

bool Buffer::empty() const {
    return _size == 0;
}

// 不移动数据，直接把data插入到head + pos位置，覆盖原有数据
void Buffer::insert(const uint8_t *data, size_t len, size_t pos) {
    if (pos >= _size) {
        // 如果位置超出了当前数据范围，直接返回
        return;
    }
    
    // 计算实际插入位置
    size_t real_pos = (head + pos) % capacity;
    
    // 检查插入后是否会超出缓冲区尾部
    if (pos + len > _size) {
        // 插入后会超出当前数据末尾，需要调整 tail 和 _size
        size_t exceed = (pos + len) - _size;
        _size = pos + len;  // 更新大小
        tail = (head + _size) % capacity;  // 更新尾部位置
    }
    
    // 检查是否需要扩容
    if (_size > capacity) {
        // 需要扩容
        size_t new_capacity = _size * 2;
        resize(new_capacity);
        
        // 重新计算 real_pos，因为 resize() 可能改变了 head 位置
        real_pos = (head + pos) % capacity;
    }
    
    // 插入数据
    if (real_pos + len > capacity) {
        // 数据将环绕缓冲区
        size_t first_part = capacity - real_pos;
        size_t second_part = len - first_part;
        
        // 复制第一部分到缓冲区末尾
        memcpy(this->data + real_pos, data, first_part);
        
        // 复制第二部分到缓冲区开始
        memcpy(this->data, data + first_part, second_part);
    } else {
        // 数据可以直接连续插入
        memcpy(this->data + real_pos, data, len);
    }
}