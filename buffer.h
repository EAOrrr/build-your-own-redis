#ifndef BUFFER_HPP
#define BUFFER_HPP
#include <cstdint>
#include <cstring>
#include <stddef.h>
#include <sys/types.h>


class Buffer {
    public:
        Buffer(size_t capacity=1024);
        ~Buffer();
        void append(const uint8_t *data, size_t len);
        void consume(size_t len);
        void append_u8(uint8_t data);
        void append_u32(uint32_t data);
        void append_i64(int64_t data);
        void append_dbl(double data);
        void insert(const uint8_t *data, size_t len, size_t pos);
        size_t size(void) const;
        bool empty(void) const;
        void peek(uint8_t* dst, size_t pos, size_t len) const;
        uint32_t peek_u32(size_t pos) const;
        void get_continuous_data(size_t pos, uint8_t **data, size_t *size) const;
        void copy_data(uint8_t *dst, size_t len) const;
        void resize(size_t new_capacity);
        uint8_t& operator[](size_t pos);
        const uint8_t& operator[](size_t pos) const;

        
    private:
        size_t head;
        size_t tail;
        size_t capacity;
        size_t _size;
        uint8_t* data;
        
};

#endif